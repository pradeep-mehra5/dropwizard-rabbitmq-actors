package io.appform.dropwizard.actors.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.dropwizard.actors.config.RMQConfig;
import io.appform.dropwizard.actors.config.RmqManagementConfig;
import io.appform.dropwizard.actors.connectivity.BindingKeyMatcher;
import io.appform.dropwizard.actors.connectivity.RmqManagementClient;
import io.appform.dropwizard.actors.connectivity.RmqManagementClient.Binding;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Applies a queue's bindings to its exchange, guarding against binding drift when the RabbitMQ Management API is
 * enabled ({@code managementConfig.enabled}).
 *
 * <p>AMQP bindings are additive and this library never unbinds, so on a durable queue a changed config would
 * silently leave stale bindings behind. With the Management API enabled, one read of the queue's bindings decides:
 * <ul>
 *   <li><b>stale exchange:</b> the queue must not be bound to any exchange other than its own (plus its own retry
 *       exchange). Checked before anything is declared or bound;</li>
 *   <li><b>immutable binding keys:</b> if the queue already has bindings from the exchange, they must equal the
 *       configured keys exactly, and nothing is (re)bound;</li>
 *   <li><b>first-creation race:</b> on first creation the keys are bound and re-read; a divergent concurrent creator
 *       (e.g. two instances with different configs) fails startup.</li>
 * </ul>
 * With the Management API disabled the keys are bound additively, without checks.
 */
@Slf4j
class BindingGuard {

    /**
     * Declares the queue (idempotent).
     */
    @FunctionalInterface
    interface QueueDeclarer {
        void declare() throws Exception;
    }

    /**
     * Binds the queue to the exchange with one key.
     */
    @FunctionalInterface
    interface QueueBinder {
        void bind(String bindingKey) throws Exception;
    }

    /**
     * Result of reading a queue's bindings. {@code bindings} is empty when the queue does not exist yet;
     * {@code available} is false when the Management API was unreachable and lenient mode allows proceeding without
     * the checks.
     */
    private record Read(boolean available, Optional<List<Binding>> bindings) {
    }

    private final RMQConfig rmqConfig;
    private final RmqManagementClient client;

    BindingGuard(final RMQConfig rmqConfig, final ObjectMapper mapper) {
        this(rmqConfig, isEnabled(rmqConfig) ? new RmqManagementClient(rmqConfig, mapper) : null);
    }

    BindingGuard(final RMQConfig rmqConfig, final RmqManagementClient client) {
        this.rmqConfig = rmqConfig;
        this.client = client;
    }

    /**
     * @param queue               broker queue name
     * @param exchange            broker exchange name
     * @param otherAllowedSources other exchanges the queue may legitimately be bound to (its own retry exchange)
     */
    void ensureBindings(final String queue,
                        final String exchange,
                        final List<String> bindingKeys,
                        final Set<String> otherAllowedSources,
                        final QueueDeclarer declarer,
                        final QueueBinder binder) throws Exception {
        if (client == null) {
            declarer.declare();
            bindAll(bindingKeys, binder);
            return;
        }
        final Read existing = read(queue, "read");
        if (existing.available() && existing.bindings().isPresent()) {
            verifyNoStaleExchange(queue, exchange, otherAllowedSources, existing.bindings().get());
        }
        declarer.declare();
        if (!existing.available()) {
            bindAll(bindingKeys, binder);
            return;
        }
        final List<String> existingKeys = keysFrom(exchange, existing.bindings());
        if (!existingKeys.isEmpty()) {
            assertMatch(bindingKeys, existingKeys, String.format(
                    "Binding keys are immutable for an existing queue. Mismatch for queue '%s' on exchange '%s'. "
                            + "To change them, use a new queue.", queue, exchange));
            return;
        }
        // First creation (or no bindings from this exchange yet): bind, then re-read to detect a divergent
        // concurrent creator.
        bindAll(bindingKeys, binder);
        final Read reread = read(queue, "re-read");
        if (reread.available() && reread.bindings().isPresent()) {
            assertMatch(bindingKeys, keysFrom(exchange, reread.bindings()), String.format(
                    "Binding keys mismatch after first-creation bind for queue '%s' on exchange '%s' "
                            + "(likely a concurrent creator with different binding keys).", queue, exchange));
        }
    }

    private static void verifyNoStaleExchange(final String queue,
                                              final String exchange,
                                              final Set<String> otherAllowed,
                                              final List<Binding> bindings) {
        final Set<String> stale = bindings.stream()
                .map(Binding::source)
                .filter(source -> !source.equals(exchange) && !otherAllowed.contains(source))
                .collect(Collectors.toSet());
        if (!stale.isEmpty()) {
            throw new IllegalStateException(String.format(
                    "Queue '%s' is bound to unexpected exchange(s) %s besides its configured exchange '%s'. "
                            + "This is a stale binding left by moving the queue to another exchange. "
                            + "To change a queue's exchange, use a new queue.", queue, stale, exchange));
        }
    }

    private static List<String> keysFrom(final String exchange, final Optional<List<Binding>> bindings) {
        return bindings.orElse(List.of()).stream()
                .filter(binding -> binding.source().equals(exchange))
                .map(Binding::routingKey)
                .collect(Collectors.toList());
    }

    private Read read(final String queue, final String phase) {
        try {
            return new Read(true, client.fetchBindings(vhost(), queue));
        } catch (RuntimeException e) {
            if (rmqConfig.getManagementConfig().isFailOnManagementUnavailable()) {
                throw new IllegalStateException(String.format(
                        "Unable to %s bindings from RabbitMQ Management API for queue '%s' (strict mode). "
                                + "Ensure the management plugin is enabled and reachable.", phase, queue), e);
            }
            log.warn("Management API unavailable on {} for queue {}; proceeding without binding drift checks",
                    phase, queue, e);
            return new Read(false, Optional.empty());
        }
    }

    private static void bindAll(final List<String> keys, final QueueBinder binder) throws Exception {
        for (final String key : keys) {
            binder.bind(key);
        }
    }

    private static void assertMatch(final List<String> configured, final List<String> actual, final String reason) {
        if (!BindingKeyMatcher.matches(configured, actual)) {
            throw new IllegalStateException(String.format("%s configured=%s, actual=%s", reason,
                    BindingKeyMatcher.normalize(configured), BindingKeyMatcher.normalize(actual)));
        }
    }

    private String vhost() {
        final String vhost = rmqConfig.getVirtualHost();
        return vhost == null ? "/" : vhost;
    }

    private static boolean isEnabled(final RMQConfig rmqConfig) {
        final RmqManagementConfig mgmt = rmqConfig == null ? null : rmqConfig.getManagementConfig();
        return mgmt != null && mgmt.isEnabled();
    }
}
