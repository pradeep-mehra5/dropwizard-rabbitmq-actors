package io.appform.dropwizard.actors.messaging.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.appform.dropwizard.actors.TtlConfig;
import io.appform.dropwizard.actors.actor.ActorConfig;
import io.appform.dropwizard.actors.actor.ConsumerConfig;
import io.appform.dropwizard.actors.actor.HaMode;
import io.appform.dropwizard.actors.actor.QueueType;
import io.appform.dropwizard.actors.exceptionhandler.config.ExceptionHandlerConfig;
import io.appform.dropwizard.actors.retry.config.NoRetryConfig;
import io.appform.dropwizard.actors.retry.config.RetryConfig;
import java.util.ArrayList;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A consumer queue bound to an exchange.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueueSpec {

    /**
     * Binding keys used to bind this queue to its exchange. Required (non-empty) for DIRECT and TOPIC exchanges,
     * must be empty for FANOUT. Immutable once the queue exists (enforced when the Management API is enabled):
     * to change them, use a new queue.
     */
    @NotNull
    @Builder.Default
    private List<String> bindingKeys = new ArrayList<>();

    @Min(1)
    @Max(100)
    @Builder.Default
    private int concurrency = 3;

    @Min(1)
    @Max(100)
    @Builder.Default
    private int prefetchCount = 1;

    @Valid
    @NotNull
    @Builder.Default
    private RetryConfig retryConfig = new NoRetryConfig();

    private ExceptionHandlerConfig exceptionHandlerConfig;

    /**
     * Consumer connection isolation and consumer tag prefix.
     */
    @Valid
    private ConsumerConfig consumer;

    /**
     * Enables {@code MessageContext.retryLater}: a private delayed exchange bound only to this queue, so a deferred
     * retry is redelivered to this queue alone and never re-broadcast to the other queues of the exchange.
     * Requires the rabbitmq_delayed_message_exchange plugin.
     */
    @Builder.Default
    private boolean retryLaterEnabled = false;

    // Queue arguments below mirror ActorConfig's on purpose (actor config is left untouched), and are mapped back to
    // it in toQueueArgumentsConfig so arguments are built by the same code as for actors. Priority queues are not
    // supported, since publishers can't set a message priority.

    @Builder.Default
    private QueueType queueType = QueueType.CLASSIC;

    /**
     * Classic mirrored queues only (deprecated in RabbitMQ 3.x, removed in 4.x).
     */
    @Builder.Default
    private HaMode haMode = HaMode.ALL;

    /**
     * Classic mirrored queues only (deprecated in RabbitMQ 3.x, removed in 4.x).
     */
    @Builder.Default
    private String haParams = "";

    @Builder.Default
    private boolean lazyMode = false;

    @Builder.Default
    private int quorumInitialGroupSize = 3;

    /**
     * Queue auto-delete: the queue (and its sideline queue) is deleted after being unused for this long
     * ({@code x-expires}). This is not a message TTL.
     */
    @Valid
    private TtlConfig ttlConfig;

    /**
     * Queue-argument view of this spec, so queue arguments (queue type, HA, auto-delete) are built exactly like they
     * are for actors.
     */
    @JsonIgnore
    public ActorConfig toQueueArgumentsConfig(final String exchange) {
        return ActorConfig.builder()
                .exchange(exchange)
                .queueType(queueType)
                .haMode(haMode)
                .haParams(haParams)
                .lazyMode(lazyMode)
                .quorumInitialGroupSize(quorumInitialGroupSize)
                .ttlConfig(ttlConfig)
                .build();
    }
}
