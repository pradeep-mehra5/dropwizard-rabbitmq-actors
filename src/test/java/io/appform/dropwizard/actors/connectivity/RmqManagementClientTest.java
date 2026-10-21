package io.appform.dropwizard.actors.connectivity;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.dropwizard.actors.common.RabbitmqActorException;
import io.appform.dropwizard.actors.config.Broker;
import io.appform.dropwizard.actors.config.RMQConfig;
import io.appform.dropwizard.actors.connectivity.RmqManagementClient.Binding;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RmqManagementClientTest {

    private MockWebServer server;
    private RmqManagementClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();

        final RMQConfig rmqConfig = new RMQConfig();
        final List<Broker> brokers = new ArrayList<>();
        brokers.add(new Broker("localhost", 5672));
        rmqConfig.setBrokers(brokers);
        rmqConfig.setUserName("guest");
        rmqConfig.setPassword("guest");
        rmqConfig.setVirtualHost("/");

        client = new RmqManagementClient(rmqConfig, new ObjectMapper(), server.url("/").toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    void testReturnsQueueBindingsExcludingDefaultExchangeAndExchangeDestinations() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "[{\"source\":\"\",\"destination_type\":\"queue\",\"routing_key\":\"q\"},"
                        + "{\"source\":\"ex\",\"destination_type\":\"queue\",\"routing_key\":\"order.*\"},"
                        + "{\"source\":\"ex\",\"destination_type\":\"queue\",\"routing_key\":\"payment.created\"},"
                        + "{\"source\":\"other\",\"destination_type\":\"queue\",\"routing_key\":\"x\"},"
                        + "{\"source\":\"ex\",\"destination_type\":\"exchange\",\"routing_key\":\"e2e\"}]"));

        final Optional<List<Binding>> bindings = client.fetchBindings("/", "q");

        Assertions.assertEquals(Optional.of(List.of(
                new Binding("ex", "order.*"),
                new Binding("ex", "payment.created"),
                new Binding("other", "x"))), bindings);
        final RecordedRequest recorded = server.takeRequest();
        // vhost "/" is a single encoded path segment
        Assertions.assertEquals("/api/queues/%2F/q/bindings", recorded.getPath());
        Assertions.assertNotNull(recorded.getHeader("Authorization"));
    }

    @Test
    void testQueueNameIsEncodedAsPathSegment() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("[]"));

        client.fetchBindings("/", "ns.my queue");

        Assertions.assertEquals("/api/queues/%2F/ns.my%20queue/bindings", server.takeRequest().getPath());
    }

    @Test
    void test404ReturnsEmpty() {
        server.enqueue(new MockResponse().setResponseCode(404));
        Assertions.assertTrue(client.fetchBindings("/", "q").isEmpty());
    }

    @Test
    void testNon200ThrowsExceptions() {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("boom"));
        Assertions.assertThrows(RabbitmqActorException.class, () -> client.fetchBindings("/", "q"));
    }

    @Test
    void testEmptyBindingsList() {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("[]"));
        Assertions.assertEquals(Optional.of(List.of()), client.fetchBindings("/", "q"));
    }

    @Test
    void testMissingRoutingKeyIsEmpty() {
        server.enqueue(new MockResponse().setResponseCode(200).setBody(
                "[{\"source\":\"fan\",\"destination_type\":\"queue\"}]"));
        Assertions.assertEquals(Optional.of(List.of(new Binding("fan", ""))), client.fetchBindings("/", "q"));
    }
}
