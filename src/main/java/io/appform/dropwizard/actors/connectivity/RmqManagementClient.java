package io.appform.dropwizard.actors.connectivity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.dropwizard.actors.common.ErrorCode;
import io.appform.dropwizard.actors.common.RabbitmqActorException;
import io.appform.dropwizard.actors.config.RMQConfig;
import io.appform.dropwizard.actors.config.RmqManagementConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Thin client over the RabbitMQ Management HTTP API, used to read a queue's current bindings (AMQP itself cannot
 * list bindings). Only the read used by the messaging binding drift checks is implemented.
 *
 * <p>Host and credentials are inherited from {@link RMQConfig} (first broker host, AMQP username / password);
 * port/scheme come from {@link RmqManagementConfig}.
 */
@Slf4j
public class RmqManagementClient {

    /**
     * One binding of a queue: the source exchange and the routing (binding) key.
     */
    public record Binding(String source, String routingKey) {
    }

    private final OkHttpClient httpClient;
    private final ObjectMapper mapper;
    private final HttpUrl baseUrl;
    private final String authHeader;

    public RmqManagementClient(final RMQConfig rmqConfig, final ObjectMapper mapper) {
        this(rmqConfig, mapper, defaultBaseUrl(rmqConfig));
    }

    /**
     * Constructor allowing an explicit base URL (e.g. for tests against a mock server).
     */
    public RmqManagementClient(final RMQConfig rmqConfig, final ObjectMapper mapper, final String baseUrl) {
        this.mapper = mapper;
        this.baseUrl = Objects.requireNonNull(HttpUrl.parse(baseUrl), () -> "Invalid management URL: " + baseUrl);
        this.authHeader = Credentials.basic(rmqConfig.getUserName(), rmqConfig.getPassword());
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .build();
    }

    private static String defaultBaseUrl(final RMQConfig rmqConfig) {
        final RmqManagementConfig mgmt = rmqConfig.getManagementConfig();
        final String scheme = mgmt != null && mgmt.isSecure() ? "https" : "http";
        final String host = rmqConfig.getBrokers().get(0).getHost();
        final int port = mgmt != null ? mgmt.getPort() : 15672;
        return String.format("%s://%s:%d", scheme, host, port);
    }

    /**
     * Reads the bindings of {@code queue} in {@code vhost}: queue-destination bindings only, excluding the implicit
     * default (nameless) exchange binding every queue has.
     *
     * @return the bindings, or {@link Optional#empty()} if the queue does not exist yet (HTTP 404).
     * @throws RabbitmqActorException on any transport error or non-200/404 response.
     */
    public Optional<List<Binding>> fetchBindings(final String vhost, final String queue) {
        final HttpUrl url = baseUrl.newBuilder()
                .addPathSegment("api")
                .addPathSegment("queues")
                .addPathSegment(vhost)
                .addPathSegment(queue)
                .addPathSegment("bindings")
                .build();
        final Request request = new Request.Builder()
                .url(url)
                .header("Accept", "application/json")
                .header("Authorization", authHeader)
                .get()
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.code() == 404) {
                return Optional.empty();
            }
            if (!response.isSuccessful() || response.body() == null) {
                throw new RabbitmqActorException(ErrorCode.INTERNAL_ERROR,
                        String.format("Management API returned %d for %s", response.code(), url), null);
            }
            final JsonNode root = mapper.readTree(response.body().string());
            final List<Binding> bindings = new ArrayList<>();
            if (root.isArray()) {
                for (final JsonNode node : root) {
                    final JsonNode source = node.get("source");
                    final JsonNode destinationType = node.get("destination_type");
                    if (source == null || destinationType == null || !"queue".equals(destinationType.asText())
                            || source.asText().isEmpty()) {
                        continue;
                    }
                    final JsonNode routingKey = node.get("routing_key");
                    bindings.add(new Binding(source.asText(), routingKey == null ? "" : routingKey.asText()));
                }
            }
            return Optional.of(bindings);
        } catch (IOException e) {
            throw new RabbitmqActorException(ErrorCode.INTERNAL_ERROR,
                    "Error calling RabbitMQ Management API: " + url, e);
        }
    }
}
