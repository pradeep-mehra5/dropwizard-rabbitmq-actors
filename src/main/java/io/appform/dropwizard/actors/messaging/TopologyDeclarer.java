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

    /**
     * Declares one exchange and everything bound to it, on a dedicated channel that is closed afterwards. Called once
     * per configured exchange when messaging starts; the config is expected to be validated already.
     *
     * @param names      logical and broker names for the exchange
     * @param spec       the exchange's config
     * @param connection connection to declare on (the exchange's producer connection)
     */
    void declare(final ExchangeNames names, final ExchangeSpec spec, final RMQConnection connection)
            throws Exception {
        final Channel channel = connection.newChannel();
        try {
            declareExchange(channel, names, spec);
            for (final Map.Entry<String, QueueSpec> entry : spec.getQueues().entrySet()) {
                declareQueue(channel, connection, names, spec, entry.getKey(), entry.getValue());
            }
        } finally {
            // A failed declaration has already closed the channel; closing it again would hide the real error.
            if (channel.isOpen()) {
                channel.close();
            }
        }
    }

    private void declareExchange(final Channel channel, final ExchangeNames names, final ExchangeSpec spec)
            throws IOException {
        // Exchange arguments are immutable: changing any of them on an existing exchange fails the redeclare (406).
        final Map<String, Object> exchangeArgs = new HashMap<>();

        // 1. Optional capture of unroutable messages. The alternate exchange must exist before the exchange that
        //    points to it.
        if (spec.isCaptureUnroutable()) {
            declareUnrouted(channel, names);
            exchangeArgs.put("alternate-exchange", names.unroutedExchange());
        }

        // 2. The exchange itself, durable and never auto-deleted. A delayed exchange is an x-delayed-message exchange
        //    that holds each message for its x-delay, then routes it like the configured type (x-delayed-type).
        if (spec.isDelayed()) {
            exchangeArgs.put(DELAYED_TYPE_ARG, spec.getType().getAmqpType());
            channel.exchangeDeclare(names.broker(), DELAYED_EXCHANGE_TYPE, true, false, exchangeArgs);
        } else {
            channel.exchangeDeclare(names.broker(), spec.getType().getAmqpType(), true, false, exchangeArgs);
        }
        log.info("Declared exchange {} (broker {}) type={} delayed={}", names.logical(), names.broker(),
                spec.getType(), spec.isDelayed());

        // 3. The sideline (dead-letter) exchange shared by the exchange's queues. Always DIRECT, whatever the exchange
        //    type: each queue dead-letters with its own name as routing key, reaching only its own sideline queue.
        channel.exchangeDeclare(names.sideline(), ExchangeType.DIRECT.getAmqpType(), true);
    }

    /**
     * Declares one queue: its sideline queue, the queue itself with its bindings (drift-guarded), and its retry
     * exchange if enabled.
     */
    private void declareQueue(final Channel channel,
                              final RMQConnection connection,
                              final ExchangeNames names,
                              final ExchangeSpec spec,
                              final String queue,
                              final QueueSpec queueSpec) throws Exception {
        final String brokerQueue = MessagingNames.queue(prefix, queue);
        final Map<String, Object> baseArgs = queueArguments(connection, names, queueSpec);

        // 1. Sideline queue first, so rejected messages always have somewhere to go.
        declareSidelineQueue(channel, names, brokerQueue, baseArgs);

        // 2. The queue itself, dead-lettering to the sideline exchange. The dead-letter routing key is pinned to the
        //    queue name: the message's own routing key (a topic key, or empty for fanout) would not match the
        //    sideline binding.
        final Map<String, Object> mainArgs = new HashMap<>(baseArgs);
        mainArgs.put("x-dead-letter-exchange", names.sideline());
        mainArgs.put("x-dead-letter-routing-key", brokerQueue);

        // 3. Bindings, through the drift guard. FANOUT ignores routing keys, so it binds with "".
        final List<String> bindingKeys = spec.getType().usesRoutingKey() ? queueSpec.getBindingKeys() : List.of("");
        final String retryExchange = MessagingNames.retryExchange(brokerQueue);
        final Set<String> allowedOtherExchanges = queueSpec.isRetryLaterEnabled() ? Set.of(retryExchange) : Set.of();
        bindingGuard.ensureBindings(brokerQueue, names.broker(), bindingKeys, allowedOtherExchanges,
                () -> channel.queueDeclare(brokerQueue, true, false, false, mainArgs),
                key -> channel.queueBind(brokerQueue, names.broker(), key));
        log.info("Declared queue {} on exchange {} with binding keys {}", brokerQueue, names.broker(), bindingKeys);

        // 4. Private retry exchange for MessageContext.retryLater.
        if (queueSpec.isRetryLaterEnabled()) {
            declareRetryExchange(channel, brokerQueue, retryExchange);
        }
    }

    /**
     * Declares {@code <queue>_SIDELINE} with the same arguments as the queue (no dead-lettering of its own), bound to
     * the exchange's sideline exchange by the queue name.
     */
    private static void declareSidelineQueue(final Channel channel,
                                             final ExchangeNames names,
                                             final String brokerQueue,
                                             final Map<String, Object> baseArgs) throws IOException {
        final String sidelineQueue = MessagingNames.sidelineQueue(brokerQueue);
        channel.queueDeclare(sidelineQueue, true, false, false, baseArgs);
        channel.queueBind(sidelineQueue, names.sideline(), brokerQueue);
    }

    /**
     * Declares the queue's private delayed exchange (routing DIRECT) and binds only this queue to it, by its name, so a
     * retry never reaches the exchange's other queues.
     */
    private static void declareRetryExchange(final Channel channel,
                                             final String brokerQueue,
                                             final String retryExchange) throws IOException {
        channel.exchangeDeclare(retryExchange, DELAYED_EXCHANGE_TYPE, true, false,
                Map.of(DELAYED_TYPE_ARG, ExchangeType.DIRECT.getAmqpType()));
        channel.queueBind(brokerQueue, retryExchange, brokerQueue);
        log.info("Declared retry exchange {} for queue {}", retryExchange, brokerQueue);
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

    /**
     * Declares the alternate exchange (FANOUT, so every unroutable message is caught whatever its routing key) and its
     * single queue.
     *
     * <p>The unrouted queue is declared without arguments, so it has no auto-delete TTL: unlike the exchange's queues,
     * it is not removed when idle (e.g. in feature environments) and has to be deleted manually.
     */
    private void declareUnrouted(final Channel channel, final ExchangeNames names) throws IOException {
        channel.exchangeDeclare(names.unroutedExchange(), ExchangeType.FANOUT.getAmqpType(), true);
        channel.queueDeclare(names.unroutedQueue(), true, false, false, Map.of());
        channel.queueBind(names.unroutedQueue(), names.unroutedExchange(), "");
        log.info("Declared unrouted capture {} -> {}", names.unroutedExchange(), names.unroutedQueue());
    }
}
