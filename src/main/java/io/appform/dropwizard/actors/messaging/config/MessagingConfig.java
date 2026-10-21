package io.appform.dropwizard.actors.messaging.config;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Exchange-centric messaging configuration: exchanges, and the queues bound to each. Validated by
 * {@link MessagingConfigValidator} when {@code Messaging} is created, which fails with the detailed list of errors.
 *
 * <pre>
 * messaging:
 *   prefix: myservice.messaging
 *   exchanges:
 *     order_events:
 *       type: TOPIC
 *       queues:
 *         shipping:
 *           bindingKeys: ["order.*.PLACED"]
 *         audit:
 *           bindingKeys: ["order.#"]
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessagingConfig {

    /**
     * Prefix for broker queue names: {@code <namespace>.<prefix>.<queue>}. The default differs from the actors'
     * default prefix, so messaging queues and actor queues don't collide by default. Don't set it to an actor prefix
     * while reusing an actor's queue name.
     */
    @NotEmpty
    @Builder.Default
    private String prefix = "rabbitmq.messaging";

    @NotNull
    @Valid
    /**
     * Exchanges keyed by logical name (used in code). The broker exchange name is the logical name prefixed with the
     * namespace, if one is set.
     */
    @Builder.Default
    private Map<String, ExchangeSpec> exchanges = new LinkedHashMap<>();
}
