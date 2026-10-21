package io.appform.dropwizard.actors.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * Configuration for talking to the RabbitMQ Management HTTP API.
 *
 * <p>AMQP has no operation to list a queue's bindings, so features that must read the current binding
 * set (the messaging binding drift checks) depend on the Management API
 * (default port 15672, requires the {@code rabbitmq_management} plugin).
 *
 * <p>This is opt-in: when {@link #enabled} is {@code false} (the default) or the config is absent, no
 * Management API calls are made and behaviour is unchanged.
 *
 * <p>Connection details:
 * <ul>
 *   <li>host: the <b>first</b> broker in {@link RMQConfig}{@code .brokers};</li>
 *   <li>credentials: the AMQP username/password. The user needs a management tag (e.g. {@code monitoring}) to
 *       read bindings;</li>
 *   <li>{@link #secure}: switches to https using the JVM's default trust store (custom AMQP cert stores are not
 *       applied).</li>
 * </ul>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RmqManagementConfig {

    /**
     * Master switch. When false (default), the Management API is never contacted.
     */
    @Default
    private boolean enabled = false;

    /**
     * Management API port (RabbitMQ default is 15672).
     */
    @Default
    @Min(1)
    @Max(65535)
    private int port = 15672;

    /**
     * Use https for the Management API endpoint.
     */
    @Default
    private boolean secure = false;

    /**
     * When the Management API is unreachable or returns an error, fail startup (strict) instead of
     * proceeding. Defaults to strict.
     */
    @Default
    private boolean failOnManagementUnavailable = true;
}
