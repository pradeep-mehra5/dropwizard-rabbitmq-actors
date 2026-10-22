package io.appform.dropwizard.actors.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;
import io.appform.dropwizard.actors.common.Constants;
import io.appform.dropwizard.actors.common.RabbitmqActorException;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

/**
 * Publishes deferred retries to a queue's private delayed retry exchange, which is bound only to that queue. If the
 * channel gets closed (e.g. a confirm timed out), it is reopened on the next retry.
 */
@Slf4j
class QueueRetryPublisher {

    private final String retryExchange;
    private final String brokerQueue;
    private final RMQConnection connection;
    private final ObjectMapper mapper;
    private final boolean confirms;
    private final long confirmTimeoutMs;
    private final Object lock = new Object();
    private Channel channel;
    private boolean started;

    QueueRetryPublisher(final String brokerQueue,
                        final RMQConnection connection,
                        final ObjectMapper mapper,
                        final boolean confirms,
                        final long confirmTimeoutMs) {
        this.retryExchange = MessagingNames.retryExchange(brokerQueue);
        this.brokerQueue = brokerQueue;
        this.connection = connection;
        this.mapper = mapper;
        this.confirms = confirms;
        this.confirmTimeoutMs = confirmTimeoutMs;
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

    void publish(final Object message, final Map<String, Object> originalHeaders, final Duration delay) {
        Preconditions.checkArgument(delay != null && !delay.isNegative(), "delay must not be negative");
        final Map<String, Object> headers = new HashMap<>();
        if (originalHeaders != null) {
            // Carry application headers forward; drop broker/library headers (x-death, x-delay, x-published-at...).
            originalHeaders.forEach((key, value) -> {
                if (!key.startsWith("x-")) {
                    headers.put(key, value);
                }
            });
        }
        headers.put(Constants.MESSAGE_PUBLISHED_TEXT, Instant.now().toEpochMilli());
        headers.put("x-delay", delay.toMillis());
        final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .deliveryMode(2)
                .messageId(UUID.randomUUID().toString())
                .headers(headers)
                .build();
        try {
            final byte[] body = mapper.writeValueAsBytes(message);
            synchronized (lock) {
                Preconditions.checkState(started, "Retry publisher for queue '%s' is not started", brokerQueue);
                if (channel == null || !channel.isOpen()) {
                    log.warn("Retry channel for queue {} is closed; reopening", brokerQueue);
                    openChannel();
                }
                channel.basicPublish(retryExchange, brokerQueue, properties, body);
                if (confirms) {
                    channel.waitForConfirmsOrDie(confirmTimeoutMs);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw RabbitmqActorException.propagate(String.format(
                    "Interrupted while scheduling retry for queue '%s'", brokerQueue), e);
        } catch (IOException | TimeoutException | ShutdownSignalException e) {
            // ShutdownSignalException is unchecked: the broker closed the channel or connection. The channel is
            // reopened on the next retry.
            throw RabbitmqActorException.propagate(String.format(
                    "Error scheduling retry for queue '%s'", brokerQueue), e);
        }
    }

    /**
     * Must be called while holding {@link #lock}.
     */
    private void openChannel() throws IOException {
        channel = connection.newChannel();
        if (confirms) {
            channel.confirmSelect();
        }
    }
}
