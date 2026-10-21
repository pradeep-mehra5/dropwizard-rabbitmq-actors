package io.appform.dropwizard.actors.messaging.config;

import io.appform.dropwizard.actors.actor.ProducerConfig;
import io.appform.dropwizard.actors.messaging.ExchangeType;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * An exchange and the queues bound to it. Every exchange must have at least one queue, consumed by this service.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExchangeSpec {

    @NotNull
    private ExchangeType type;

    /**
     * Declare the exchange as an {@code x-delayed-message} exchange (rabbitmq_delayed_message_exchange plugin) so
     * events can be published with a delay. The plugin does not support the {@code mandatory} flag, so a delayed
     * exchange must also set {@link #captureUnroutable} to catch unroutable events.
     */
    @Builder.Default
    private boolean delayed = false;

    /**
     * Declare an alternate exchange ({@code <exchange>_UNROUTED}, fanout) with a queue that receives every message
     * no binding matched, instead of the broker dropping it. Exchange arguments are immutable: changing this on an
     * existing exchange requires recreating the exchange.
     */
    @Builder.Default
    private boolean captureUnroutable = false;

    /**
     * Wait for the broker's publisher confirm on every publish (default). A publish then fails if the broker did not
     * accept it, or with {@code UnroutableMessageException} if no binding matched (and it was not captured by an
     * alternate exchange). Costs a broker round trip per publish; disable for fire-and-forget throughput.
     */
    @Builder.Default
    private boolean publisherConfirms = true;

    @Min(100)
    @Max(60_000)
    @Builder.Default
    private long confirmTimeoutMs = 5_000;

    /**
     * Producer connection isolation.
     */
    @Valid
    private ProducerConfig producer;

    /**
     * Queues bound to this exchange, keyed by logical queue name.
     */
    @NotNull
    @Valid
    @Builder.Default
    private Map<String, QueueSpec> queues = new LinkedHashMap<>();
}
