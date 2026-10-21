package io.appform.dropwizard.actors.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Return;
import io.appform.dropwizard.actors.common.Constants;
import io.appform.dropwizard.actors.common.RabbitmqActorException;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.messaging.MessagingNames.ExchangeNames;
import io.appform.dropwizard.actors.messaging.config.ExchangeSpec;
import io.appform.dropwizard.actors.observers.PublishObserverContext;
import io.appform.dropwizard.actors.observers.RMQObserver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

/**
 * Publishes events to an exchange. Which queues receive an event is decided by the exchange type and the queues'
 * bindings, never by the publisher.
 *
 * <p>Delivery guarantees (non-delayed exchanges):
 * <ul>
 *   <li>every publish uses {@code mandatory=true}; if no binding matches, the broker returns the message;</li>
 *   <li>with {@code publisherConfirms} (default) the publish blocks until the broker confirms it, and a returned
 *       (unroutable) message fails the publish with {@link UnroutableMessageException};</li>
 *   <li>without confirms, returned messages are only logged (the publish has already returned).</li>
 * </ul>
 * The delayed-message plugin does not support {@code mandatory}, so on a delayed exchange an unroutable event is
 * not detected: it is dropped silently unless {@code captureUnroutable} is set.
 *
 * <p>Thread-safe: publishes are serialized on one channel. If the channel gets closed (e.g. a confirm timed out or the
 * broker rejected an operation), it is reopened on the next publish.
 */
@Slf4j
public class EventPublisher<E> {

    /**
     * AMQP limits a routing key (short string) to 255 bytes.
     */
    static final int MAX_ROUTING_KEY_BYTES = 255;

    private final ExchangeNames names;
    private final ExchangeSpec spec;
    private final RoutingKeyResolver<E> routingKeyResolver;
    private final RMQConnection connection;
    private final ObjectMapper mapper;
    private final RMQObserver observer;
    private final Object lock = new Object();

    private Channel channel;
    private boolean started;
    private volatile Return lastReturn;

    EventPublisher(final ExchangeNames names,
                   final ExchangeSpec spec,
                   final RoutingKeyResolver<E> routingKeyResolver,
                   final RMQConnection connection,
                   final ObjectMapper mapper) {
        Preconditions.checkArgument(!spec.getType().usesRoutingKey() || routingKeyResolver != null,
                "A RoutingKeyResolver is required to publish to %s exchange '%s'", spec.getType(), names.logical());
        Preconditions.checkArgument(spec.getType().usesRoutingKey() || routingKeyResolver == null,
                "FANOUT exchange '%s' ignores routing keys; do not pass a RoutingKeyResolver", names.logical());
        this.names = names;
        this.spec = spec;
        this.routingKeyResolver = routingKeyResolver;
        this.connection = connection;
        this.mapper = mapper;
        this.observer = connection.getRootObserver();
    }

    /**
     * Logical exchange name (as used in code).
     */
    public String getExchange() {
        return names.logical();
    }

    void start() throws IOException {
        synchronized (lock) {
            openChannel();
            started = true;
        }
    }

    void stop() throws IOException, TimeoutException {
        synchronized (lock) {
            started = false;
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        }
    }

    public void publish(final E event) {
        publish(event, Map.of());
    }

    public void publish(final E event, final Map<String, Object> headers) {
        send(event, headers, null, null);
    }

    /**
     * Publishes with a delay. Requires the exchange to be configured with {@code delayed: true}.
     */
    public void publishWithDelay(final E event, final Duration delay) {
        Preconditions.checkState(spec.isDelayed(),
                "Exchange '%s' is not delayed; set delayed: true to publish with a delay", names.logical());
        Preconditions.checkArgument(delay != null && !delay.isNegative(), "delay must not be negative");
        send(event, Map.of(), delay, null);
    }

    /**
     * Publishes an event that consumers treat as expired after {@code expiry}: it is handed to
     * {@code QueueHandler.handleExpired} instead of {@code handle}.
     */
    public void publishWithExpiry(final E event, final Duration expiry) {
        Preconditions.checkArgument(expiry != null && !expiry.isNegative() && !expiry.isZero(),
                "expiry must be positive");
        send(event, Map.of(), null, expiry);
    }

    private void send(final E event, final Map<String, Object> headers, final Duration delay, final Duration expiry) {
        Objects.requireNonNull(event, "event");
        final String routingKey = routingKey(event);
        final AMQP.BasicProperties properties = properties(headers, delay, expiry);
        final PublishObserverContext context = PublishObserverContext.builder()
                .queueName(names.broker())
                .messageProperties(properties)
                .build();
        observer.executePublish(context, details -> {
            doSend(event, routingKey, details.getMessageProperties());
            return null;
        });
    }

    private void doSend(final E event, final String routingKey, final AMQP.BasicProperties properties) {
        // The delayed-message plugin does not support mandatory: it would return every message.
        final boolean mandatory = !spec.isDelayed();
        try {
            final byte[] body = mapper.writeValueAsBytes(event);
            synchronized (lock) {
                Preconditions.checkState(started, "Publisher for exchange '%s' is not started", names.logical());
                if (channel == null || !channel.isOpen()) {
                    log.warn("Publish channel for exchange {} is closed; reopening", names.logical());
                    openChannel();
                }
                lastReturn = null;
                channel.basicPublish(names.broker(), routingKey, mandatory, properties, body);
                if (spec.isPublisherConfirms()) {
                    // The broker sends basic.return for an unroutable mandatory message before its confirm.
                    channel.waitForConfirmsOrDie(spec.getConfirmTimeoutMs());
                    final Return returned = lastReturn;
                    if (returned != null
                            && Objects.equals(returned.getProperties().getMessageId(), properties.getMessageId())) {
                        throw new UnroutableMessageException(names.logical(), names.broker(), routingKey,
                                returned.getReplyText());
                    }
                }
            }
        } catch (TimeoutException e) {
            throw RabbitmqActorException.propagate(String.format(
                    "Timed out waiting for publisher confirm on exchange '%s'", names.logical()), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw RabbitmqActorException.propagate(String.format(
                    "Interrupted while publishing to exchange '%s'", names.logical()), e);
        } catch (IOException e) {
            throw RabbitmqActorException.propagate(String.format(
                    "Error publishing to exchange '%s'", names.logical()), e);
        }
    }

    /**
     * Must be called while holding {@link #lock}.
     */
    private void openChannel() throws IOException {
        channel = connection.newChannel();
        if (spec.isPublisherConfirms()) {
            channel.confirmSelect();
        }
        channel.addReturnListener(this::onReturn);
    }

    private void onReturn(final Return returned) {
        lastReturn = returned;
        log.error("Unroutable message returned by broker: exchange={} routingKey={} replyCode={} replyText={} "
                        + "messageId={}", returned.getExchange(), returned.getRoutingKey(), returned.getReplyCode(),
                returned.getReplyText(), returned.getProperties().getMessageId());
    }

    private String routingKey(final E event) {
        if (!spec.getType().usesRoutingKey()) {
            return "";
        }
        final String key = routingKeyResolver.resolve(event);
        Preconditions.checkArgument(key != null && !key.isBlank(),
                "RoutingKeyResolver returned a blank routing key for exchange '%s'", names.logical());
        Preconditions.checkArgument(key.getBytes(StandardCharsets.UTF_8).length <= MAX_ROUTING_KEY_BYTES,
                "Routing key for exchange '%s' exceeds %s bytes: %s", names.logical(), MAX_ROUTING_KEY_BYTES, key);
        return key;
    }

    private static AMQP.BasicProperties properties(final Map<String, Object> headers,
                                                   final Duration delay,
                                                   final Duration expiry) {
        final Map<String, Object> allHeaders = new HashMap<>(headers);
        final long now = Instant.now().toEpochMilli();
        allHeaders.put(Constants.MESSAGE_PUBLISHED_TEXT, now);
        if (delay != null) {
            allHeaders.put("x-delay", delay.toMillis());
        }
        if (expiry != null) {
            allHeaders.put(Constants.MESSAGE_EXPIRY_TEXT, now + expiry.toMillis());
        }
        return new AMQP.BasicProperties.Builder()
                .deliveryMode(2)
                .messageId(UUID.randomUUID().toString())
                .headers(allHeaders)
                .build();
    }
}
