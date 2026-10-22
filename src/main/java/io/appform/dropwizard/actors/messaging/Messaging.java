package io.appform.dropwizard.actors.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.appform.dropwizard.actors.ConnectionRegistry;
import io.appform.dropwizard.actors.base.utils.NamingUtils;
import io.appform.dropwizard.actors.config.RMQConfig;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.exceptionhandler.ExceptionHandlingFactory;
import io.appform.dropwizard.actors.messaging.config.ExchangeSpec;
import io.appform.dropwizard.actors.messaging.config.MessagingConfig;
import io.appform.dropwizard.actors.messaging.config.MessagingConfigValidator;
import io.appform.dropwizard.actors.messaging.config.QueueSpec;
import io.appform.dropwizard.actors.retry.RetryStrategyFactory;
import io.dropwizard.lifecycle.Managed;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Exchange-centric messaging: application code publishes messages to exchanges, and handlers consume from the queues
 * bound to those exchanges. Publishers and consumers are separate, so a call site always names the exchange it
 * publishes to, and the configured bindings alone decide which queues receive a message.
 *
 * <p>Register publishers and handlers before the application starts (e.g. in {@code Application.run}):
 * <pre>
 * MessagePublisher&lt;OrderMessage&gt; publisher = messaging.publisher("order_events", OrderMessage::routingKey);
 * messaging.consumer("order_events", "shipping", OrderMessage.class, (message, context) -&gt; ship(message));
 * </pre>
 *
 * <p>{@link #start()} then:
 * <ol>
 *   <li>checks that every configured queue has a handler (every exchange has at least one queue, so an exchange is
 *       only published to when this service also consumes from it);</li>
 *   <li>declares the whole topology (exchanges, queues, bindings, sideline, retry and unrouted capture), so bindings
 *       exist before the first publish;</li>
 *   <li>starts publishers, then consumers.</li>
 * </ol>
 *
 * <p>Messaging declares only what is in its config; actor topology is untouched. A queue belongs either to an actor
 * or to messaging, never both.
 */
@Slf4j
public class Messaging implements Managed {

    private final MessagingConfig config;
    private final ConnectionRegistry connectionRegistry;
    private final ObjectMapper mapper;
    private final RetryStrategyFactory retryStrategyFactory;
    private final ExceptionHandlingFactory exceptionHandlingFactory;
    private final TopologyDeclarer topologyDeclarer;

    private final List<MessagePublisher<?>> publishers = new ArrayList<>();
    private final Map<String, QueueConsumer<?>> consumers = new LinkedHashMap<>();
    private volatile boolean started;

    public Messaging(final MessagingConfig config,
                     final RMQConfig rmqConfig,
                     final ConnectionRegistry connectionRegistry,
                     final ObjectMapper mapper) {
        this(config, connectionRegistry, mapper, new RetryStrategyFactory(), new ExceptionHandlingFactory(),
                new TopologyDeclarer(config.getPrefix(), new BindingGuard(rmqConfig, mapper)));
    }

    Messaging(final MessagingConfig config,
              final ConnectionRegistry connectionRegistry,
              final ObjectMapper mapper,
              final RetryStrategyFactory retryStrategyFactory,
              final ExceptionHandlingFactory exceptionHandlingFactory,
              final TopologyDeclarer topologyDeclarer) {
        final List<String> errors = MessagingConfigValidator.validate(config);
        Preconditions.checkArgument(errors.isEmpty(), "Invalid messaging config: %s", errors);
        this.config = config;
        this.connectionRegistry = connectionRegistry;
        this.mapper = mapper;
        this.retryStrategyFactory = retryStrategyFactory;
        this.exceptionHandlingFactory = exceptionHandlingFactory;
        this.topologyDeclarer = topologyDeclarer;
    }

    /**
     * Publisher for a DIRECT or TOPIC exchange.
     */
    public synchronized <M> MessagePublisher<M> publisher(final String exchange,
                                                        final RoutingKeyResolver<M> routingKeyResolver) {
        Preconditions.checkNotNull(routingKeyResolver, "routingKeyResolver");
        return register(exchange, routingKeyResolver);
    }

    /**
     * Publisher for a FANOUT exchange.
     */
    public synchronized <M> MessagePublisher<M> publisher(final String exchange) {
        return register(exchange, null);
    }

    /**
     * Registers the handler for a configured queue. Each queue has exactly one handler.
     */
    public synchronized <M> void consumer(final String exchange,
                                          final String queue,
                                          final Class<? extends M> messageType,
                                          final QueueHandler<M> handler) {
        checkNotStarted();
        Preconditions.checkNotNull(messageType, "messageType");
        Preconditions.checkNotNull(handler, "handler");
        final ExchangeSpec exchangeSpec = exchangeSpec(exchange);
        final QueueSpec queueSpec = exchangeSpec.getQueues().get(queue);
        Preconditions.checkArgument(queueSpec != null, "Queue '%s' is not configured under exchange '%s'",
                queue, exchange);
        Preconditions.checkArgument(!consumers.containsKey(queue), "A handler is already registered for queue '%s'",
                queue);
        consumers.put(queue, new QueueConsumer<>(config.getPrefix(), queue, exchangeSpec, queueSpec, messageType,
                handler,
                connectionRegistry.createOrGet(NamingUtils.consumerConnectionName(queueSpec.getConsumer())),
                producerConnection(exchangeSpec), mapper, retryStrategyFactory, exceptionHandlingFactory));
    }

    @Override
    public synchronized void start() throws Exception {
        checkNotStarted();
        final List<String> unhandled = config.getExchanges().values().stream()
                .flatMap(spec -> spec.getQueues().keySet().stream())
                .filter(queue -> !consumers.containsKey(queue))
                .collect(Collectors.toList());
        Preconditions.checkState(unhandled.isEmpty(),
                "No handler registered for configured queue(s) %s; messages routed to them would never be consumed",
                unhandled);

        final List<MessagePublisher<?>> startedPublishers = new ArrayList<>();
        final List<QueueConsumer<?>> startedConsumers = new ArrayList<>();
        try {
            for (final Map.Entry<String, ExchangeSpec> entry : config.getExchanges().entrySet()) {
                topologyDeclarer.declare(names(entry.getKey()), entry.getValue(), producerConnection(entry.getValue()));
            }
            for (final MessagePublisher<?> publisher : publishers) {
                publisher.start();
                startedPublishers.add(publisher);
            }
            for (final QueueConsumer<?> consumer : consumers.values()) {
                consumer.start();
                startedConsumers.add(consumer);
            }
        } catch (Exception e) {
            log.error("Messaging failed to start; stopping what was started", e);
            stopAll(startedConsumers, startedPublishers);
            throw e;
        }
        started = true;
        log.info("Messaging started: exchanges={} publishers={} consumers={}",
                config.getExchanges().keySet(), publishers.size(), consumers.keySet());
    }

    @Override
    public synchronized void stop() throws Exception {
        stopAll(consumers.values(), publishers);
        started = false;
    }

    /**
     * Stops consumers first (their retry publishers with them), then publishers. Keeps going on failure and rethrows
     * the first error at the end.
     */
    private static void stopAll(final Collection<? extends QueueConsumer<?>> consumersToStop,
                                final Collection<? extends MessagePublisher<?>> publishersToStop) throws Exception {
        Exception first = null;
        for (final QueueConsumer<?> consumer : consumersToStop) {
            try {
                consumer.stop();
            } catch (Exception e) {
                first = first == null ? e : first;
                log.error("Error stopping consumer", e);
            }
        }
        for (final MessagePublisher<?> publisher : publishersToStop) {
            try {
                publisher.stop();
            } catch (Exception e) {
                first = first == null ? e : first;
                log.error("Error stopping publisher for exchange {}", publisher.getExchange(), e);
            }
        }
        if (first != null) {
            throw first;
        }
    }

    private <M> MessagePublisher<M> register(final String exchange, final RoutingKeyResolver<M> routingKeyResolver) {
        checkNotStarted();
        final ExchangeSpec spec = exchangeSpec(exchange);
        final MessagePublisher<M> publisher = new MessagePublisher<>(names(exchange), spec, routingKeyResolver,
                producerConnection(spec), mapper);
        publishers.add(publisher);
        return publisher;
    }

    private MessagingNames.ExchangeNames names(final String exchange) {
        return MessagingNames.forExchange(config.getPrefix(), exchange);
    }

    private ExchangeSpec exchangeSpec(final String exchange) {
        final ExchangeSpec spec = config.getExchanges().get(exchange);
        Preconditions.checkArgument(spec != null, "Exchange '%s' is not configured under messaging.exchanges",
                exchange);
        return spec;
    }

    private RMQConnection producerConnection(final ExchangeSpec spec) {
        return connectionRegistry.createOrGet(NamingUtils.producerConnectionName(spec.getProducer()));
    }

    private void checkNotStarted() {
        Preconditions.checkState(!started, "Publishers and handlers must be registered before messaging starts");
    }
}
