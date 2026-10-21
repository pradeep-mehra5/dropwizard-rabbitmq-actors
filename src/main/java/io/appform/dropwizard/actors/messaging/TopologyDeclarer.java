package io.appform.dropwizard.actors.messaging;

import com.rabbitmq.client.Channel;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.messaging.MessagingNames.ExchangeNames;
import io.appform.dropwizard.actors.messaging.config.ExchangeSpec;
import io.appform.dropwizard.actors.messaging.config.QueueSpec;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Declares an exchange and everything bound to it, in this order:
 * <ol>
 *   <li>alternate exchange + unrouted queue (if {@code captureUnroutable});</li>
 *   <li>the exchange (optionally {@code x-delayed-message});</li>
 *   <li>the shared sideline (dead-letter) exchange (DIRECT);</li>
 *   <li>per queue: sideline queue, main queue (dead-lettering to its sideline queue), bindings (drift-guarded) and the
 *       private retry exchange (if {@code retryLaterEnabled}).</li>
 * </ol>
 * This replaces, for messaging, what {@code UnmanagedPublisher.start()} does for a single actor. Actor topology is not
 * touched.
 *
 * <p>Declarations run on a dedicated channel that is closed afterwards: the broker closes a channel on any declaration
 * error (e.g. {@code 406 PRECONDITION_FAILED}), and that must not take down the connection's shared channel used by
 * actors.
 */
@Slf4j
class TopologyDeclarer {

    static final String DELAYED_EXCHANGE_TYPE = "x-delayed-message";
    static final String DELAYED_TYPE_ARG = "x-delayed-type";
    static final String HA_MODE_ARG = "ha-mode";
    static final String HA_PARAMS_ARG = "ha-params";

    private final String prefix;
    private final BindingGuard bindingGuard;

    TopologyDeclarer(final String prefix, final BindingGuard bindingGuard) {
        this.prefix = prefix;
        this.bindingGuard = bindingGuard;
    }

    void declare(final ExchangeNames names, final ExchangeSpec spec, final RMQConnection connection)
            throws Exception {
        final Channel channel = connection.newChannel();
        try {
            declareExchange(channel, names, spec);
            for (final Map.Entry<String, QueueSpec> entry : spec.getQueues().entrySet()) {
                declareQueue(channel, connection, names, spec, entry.getKey(), entry.getValue());
            }
        } finally {
            if (channel.isOpen()) {
                channel.close();
            }
        }
    }

    private void declareExchange(final Channel channel, final ExchangeNames names, final ExchangeSpec spec)
            throws IOException {
        final Map<String, Object> exchangeArgs = new HashMap<>();
        if (spec.isCaptureUnroutable()) {
            declareUnrouted(channel, names);
            exchangeArgs.put("alternate-exchange", names.unroutedExchange());
        }
        if (spec.isDelayed()) {
            exchangeArgs.put(DELAYED_TYPE_ARG, spec.getType().getAmqpType());
            channel.exchangeDeclare(names.broker(), DELAYED_EXCHANGE_TYPE, true, false, exchangeArgs);
        } else {
            channel.exchangeDeclare(names.broker(), spec.getType().getAmqpType(), true, false, exchangeArgs);
        }
        log.info("Declared exchange {} (broker {}) type={} delayed={}", names.logical(), names.broker(),
                spec.getType(), spec.isDelayed());
        channel.exchangeDeclare(names.sideline(), ExchangeType.DIRECT.getAmqpType(), true);
    }

    private void declareQueue(final Channel channel,
                              final RMQConnection connection,
                              final ExchangeNames names,
                              final ExchangeSpec spec,
                              final String queue,
                              final QueueSpec queueSpec) throws Exception {
        final String brokerQueue = MessagingNames.queue(prefix, queue);
        final Map<String, Object> baseArgs = queueArguments(connection, names, queueSpec);

        final String sidelineQueue = MessagingNames.sidelineQueue(brokerQueue);
        channel.queueDeclare(sidelineQueue, true, false, false, baseArgs);
        channel.queueBind(sidelineQueue, names.sideline(), brokerQueue);

        // The dead-letter routing key is pinned to the queue name: the message's own routing key (a topic key, or
        // empty for fanout) would not match the sideline binding.
        final Map<String, Object> mainArgs = new HashMap<>(baseArgs);
        mainArgs.put("x-dead-letter-exchange", names.sideline());
        mainArgs.put("x-dead-letter-routing-key", brokerQueue);

        final List<String> bindingKeys = spec.getType().usesRoutingKey() ? queueSpec.getBindingKeys() : List.of("");
        final String retryExchange = MessagingNames.retryExchange(brokerQueue);
        final Set<String> otherSources = queueSpec.isRetryLaterEnabled() ? Set.of(retryExchange) : Set.of();

        bindingGuard.ensureBindings(brokerQueue, names.broker(), bindingKeys, otherSources,
                () -> channel.queueDeclare(brokerQueue, true, false, false, mainArgs),
                key -> channel.queueBind(brokerQueue, names.broker(), key));
        log.info("Declared queue {} on exchange {} with binding keys {}", brokerQueue, names.broker(), bindingKeys);

        if (queueSpec.isRetryLaterEnabled()) {
            channel.exchangeDeclare(retryExchange, DELAYED_EXCHANGE_TYPE, true, false,
                    Map.of(DELAYED_TYPE_ARG, ExchangeType.DIRECT.getAmqpType()));
            channel.queueBind(brokerQueue, retryExchange, brokerQueue);
            log.info("Declared retry exchange {} for queue {}", retryExchange, brokerQueue);
        }
    }

    /**
     * Queue arguments built by the same code as for actors, minus {@code ha-mode}/{@code ha-params}: the actor builder
     * always adds them for classic queues, but as queue arguments they don't enable mirroring (RabbitMQ mirrors classic
     * queues only through policies), so messaging queues are declared without them.
     */
    private static Map<String, Object> queueArguments(final RMQConnection connection,
                                                      final ExchangeNames names,
                                                      final QueueSpec queueSpec) {
        final Map<String, Object> args = new HashMap<>(
                connection.rmqOpts(queueSpec.toQueueArgumentsConfig(names.broker())));
        args.remove(HA_MODE_ARG);
        args.remove(HA_PARAMS_ARG);
        return args;
    }

    private void declareUnrouted(final Channel channel, final ExchangeNames names) throws IOException {
        channel.exchangeDeclare(names.unroutedExchange(), ExchangeType.FANOUT.getAmqpType(), true);
        channel.queueDeclare(names.unroutedQueue(), true, false, false, Map.of());
        channel.queueBind(names.unroutedQueue(), names.unroutedExchange(), "");
        log.info("Declared unrouted capture {} -> {}", names.unroutedExchange(), names.unroutedQueue());
    }
}
