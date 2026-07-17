package io.appform.dropwizard.actors;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.rabbitmq.client.Channel;
import io.appform.dropwizard.actors.actor.ActorConfig;
import io.appform.dropwizard.actors.actor.DelayType;
import io.appform.dropwizard.actors.actor.ExchangeType;
import io.appform.dropwizard.actors.base.RandomShardIdCalculator;
import io.appform.dropwizard.actors.base.RoutingKeyResolver;
import io.appform.dropwizard.actors.base.UnmanagedPublisher;
import io.appform.dropwizard.actors.base.utils.NamingUtils;
import io.appform.dropwizard.actors.config.MetricConfig;
import io.appform.dropwizard.actors.config.RMQConfig;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.connectivity.actor.RabbitMQBundleTestAppConfiguration;
import io.appform.dropwizard.actors.observers.ThreadLocalObserver;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ExchangeTypesTest {

    private RMQConnection connection;
    private Channel channel;
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setup() throws Exception {
        val config = RMQConfig.builder()
                .brokers(new java.util.ArrayList<>())
                .userName("")
                .threadPoolSize(1)
                .password("")
                .secure(false)
                .startupGracePeriodSeconds(1)
                .metricConfig(MetricConfig.builder().enabledForAll(true).build())
                .build();
        val actorBundleImpl = new RabbitmqActorBundle<RabbitMQBundleTestAppConfiguration>() {
            @Override
            protected TtlConfig ttlConfig() {
                return TtlConfig.builder().build();
            }

            @Override
            protected RMQConfig getConfig(RabbitMQBundleTestAppConfiguration c) {
                return config;
            }
        };
        this.connection = Mockito.mock(RMQConnection.class);
        this.channel = Mockito.mock(Channel.class);
        this.objectMapper = new ObjectMapper();
        val environment = Mockito.mock(Environment.class);
        val lifecycle = Mockito.mock(LifecycleEnvironment.class);
        Mockito.doReturn(new MetricRegistry()).when(environment).metrics();
        Mockito.doReturn(lifecycle).when(environment).lifecycle();
        Mockito.doNothing().when(lifecycle).manage(ArgumentMatchers.any(ConnectionRegistry.class));
        actorBundleImpl.registerObserver(new ThreadLocalObserver(null));
        actorBundleImpl.run(new RabbitMQBundleTestAppConfiguration(), environment);

        Mockito.doReturn(actorBundleImpl.getConnectionRegistry().getRootObserver()).when(connection).getRootObserver();
        Mockito.doReturn(channel).when(connection).newChannel();
        Mockito.doReturn(channel).when(connection).channel();
    }

    @Test
    void testDirectExchangeDeclaredByDefault() throws Exception {
        val actorConfig = new ActorConfig();
        actorConfig.setExchange("direct-exchange");
        val queueName = NamingUtils.queueName(actorConfig.getPrefix(), "direct-queue");
        val publisher = newPublisher("direct-queue", actorConfig, null);
        publisher.start();

        verify(channel, times(1)).exchangeDeclare("direct-exchange", "direct", true);
        // Backward compatibility: DIRECT must NOT pin x-dead-letter-routing-key (would break redeclare of
        // pre-existing queues). It must use the original 2-arg rmqOpts (dlx only).
        verify(connection, times(1)).rmqOpts(
                ArgumentMatchers.eq(NamingUtils.getSideline("direct-exchange")),
                ArgumentMatchers.eq(actorConfig));
        verify(connection, org.mockito.Mockito.never()).rmqOpts(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.eq(queueName),
                ArgumentMatchers.eq(actorConfig));
    }

    @Test
    void testTopicExchangeDeclaredWithBindingKeys() throws Exception {
        val actorConfig = new ActorConfig();
        actorConfig.setExchange("topic-exchange");
        actorConfig.setExchangeType(ExchangeType.TOPIC);
        actorConfig.setBindingKeys(List.of("orders.*", "orders.created"));
        val queueName = NamingUtils.queueName(actorConfig.getPrefix(), "topic-queue");
        val publisher = newPublisher("topic-queue", actorConfig, message -> "orders.created");
        publisher.start();

        verify(channel, times(1)).exchangeDeclare("topic-exchange", "topic", true);
        verify(connection, times(1)).ensureWithBindingKeys(
                ArgumentMatchers.eq(queueName),
                ArgumentMatchers.eq("topic-exchange"),
                ArgumentMatchers.eq(List.of("orders.*", "orders.created")),
                ArgumentMatchers.any());
        // Dead-letter routing key must be pinned to the queue name so dead-lettered messages
        // route to the sideline queue on the direct _SIDELINE exchange regardless of the topic routing key.
        verify(connection, times(1)).rmqOpts(
                ArgumentMatchers.eq(NamingUtils.getSideline("topic-exchange")),
                ArgumentMatchers.eq(queueName),
                ArgumentMatchers.eq(actorConfig));
    }

    @Test
    void testTopicWithoutResolverFails() {
        val actorConfig = new ActorConfig();
        actorConfig.setExchange("topic-exchange");
        actorConfig.setExchangeType(ExchangeType.TOPIC);
        val publisher = newPublisher("topic-queue", actorConfig, null);
        Assertions.assertThrows(IllegalStateException.class, publisher::start);
    }

    @Test
    void testTtlDelayWithTopicFails() {
        val actorConfig = new ActorConfig();
        actorConfig.setExchange("topic-exchange");
        actorConfig.setExchangeType(ExchangeType.TOPIC);
        actorConfig.setDelayed(true);
        actorConfig.setDelayType(DelayType.TTL);
        val publisher = newPublisher("topic-queue", actorConfig, message -> "rk");
        Assertions.assertThrows(IllegalStateException.class, publisher::start);
    }

    @Test
    void testTtlDelayWithFanoutFails() {
        val actorConfig = new ActorConfig();
        actorConfig.setExchange("fanout-exchange");
        actorConfig.setExchangeType(ExchangeType.FANOUT);
        actorConfig.setDelayed(true);
        actorConfig.setDelayType(DelayType.TTL);
        val publisher = newPublisher("fanout-queue", actorConfig, null);
        Assertions.assertThrows(IllegalStateException.class, publisher::start);
    }

    @Test
    void testFanoutExchangeDeclaredWithEmptyBindingKey() throws Exception {
        val actorConfig = new ActorConfig();
        actorConfig.setExchange("fanout-exchange");
        actorConfig.setExchangeType(ExchangeType.FANOUT);
        val queueName = NamingUtils.queueName(actorConfig.getPrefix(), "fanout-queue");
        val publisher = newPublisher("fanout-queue", actorConfig, null);
        publisher.start();

        verify(channel, times(1)).exchangeDeclare("fanout-exchange", "fanout", true);
        verify(connection, times(1)).ensureWithBindingKeys(
                ArgumentMatchers.eq(queueName),
                ArgumentMatchers.eq("fanout-exchange"),
                ArgumentMatchers.eq(List.of("")),
                ArgumentMatchers.any());
    }

    @Test
    void testDelayedTopicExchangeUsesTopicDelayType() throws Exception {
        val actorConfig = new ActorConfig();
        actorConfig.setExchange("delayed-topic");
        actorConfig.setExchangeType(ExchangeType.TOPIC);
        actorConfig.setDelayed(true);
        actorConfig.setDelayType(DelayType.DELAYED);
        val publisher = newPublisher("delayed-topic-queue", actorConfig, message -> "rk");
        publisher.start();

        verify(channel, times(1)).exchangeDeclare(
                ArgumentMatchers.eq("delayed-topic"),
                ArgumentMatchers.eq("x-delayed-message"),
                ArgumentMatchers.eq(true),
                ArgumentMatchers.eq(false),
                ArgumentMatchers.eq(ImmutableMap.of("x-delayed-type", "topic")));
    }

    @Test
    void testSidelineAlwaysDirect() throws Exception {
        val actorConfig = new ActorConfig();
        actorConfig.setExchange("topic-exchange");
        actorConfig.setExchangeType(ExchangeType.TOPIC);
        val publisher = newPublisher("topic-queue", actorConfig, message -> "rk");
        publisher.start();

        verify(channel, times(1))
                .exchangeDeclare(NamingUtils.getSideline("topic-exchange"), "direct", true);
    }

    private UnmanagedPublisher<Object> newPublisher(final String name,
                                                    final ActorConfig actorConfig,
                                                    final RoutingKeyResolver<Object> resolver) {
        return new UnmanagedPublisher<>(NamingUtils.queueName(actorConfig.getPrefix(), name), actorConfig,
                new RandomShardIdCalculator<>(actorConfig), resolver, connection,
                objectMapper);
    }
}
