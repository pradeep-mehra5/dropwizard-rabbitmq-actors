package io.appform.dropwizard.actors.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import io.appform.dropwizard.actors.actor.ActorConfig;
import io.appform.dropwizard.actors.actor.DelayType;
import io.appform.dropwizard.actors.actor.ExchangeType;
import io.appform.dropwizard.actors.actor.ExchangeTypeBindingKeysVisitor;
import io.appform.dropwizard.actors.actor.ExchangeTypeVisitor;
import io.appform.dropwizard.actors.base.utils.NamingUtils;
import io.appform.dropwizard.actors.common.Constants;
import io.appform.dropwizard.actors.common.RabbitmqActorException;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.observers.PublishObserverContext;
import io.appform.dropwizard.actors.observers.RMQObserver;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;

@Slf4j
public class UnmanagedPublisher<Message> {

    @Getter
    private final String queueName;
    private final ActorConfig config;
    private final RMQConnection connection;
    private final ObjectMapper mapper;
    private final ShardIdCalculator<Message> shardIdCalculator;
    private final RoutingKeyResolver<Message> routingKeyResolver;
    private final RMQObserver observer;
    private Channel publishChannel;

    public UnmanagedPublisher(
            String name,
            ActorConfig config,
            RMQConnection connection,
            ObjectMapper mapper) {
        this(name,
             config,
             new RandomShardIdCalculator<>(config),
             connection,
             mapper);
    }

    public UnmanagedPublisher(final String queueName,
                              final ActorConfig config,
                              final ShardIdCalculator<Message> shardIdCalculator,
                              final RMQConnection connection,
                              final ObjectMapper mapper) {
        this(queueName, config, shardIdCalculator, null, connection, mapper);
    }

    public UnmanagedPublisher(final String queueName,
                              final ActorConfig config,
                              final ShardIdCalculator<Message> shardIdCalculator,
                              final RoutingKeyResolver<Message> routingKeyResolver,
                              final RMQConnection connection,
                              final ObjectMapper mapper) {
        this.queueName = queueName;
        this.config = config;
        this.shardIdCalculator = shardIdCalculator;
        this.routingKeyResolver = routingKeyResolver;
        this.connection = connection;
        this.mapper = mapper;
        this.observer = connection.getRootObserver();
    }

    public final void publishWithDelay(final Message message, final long delayMilliseconds) throws Exception {
        log.info("Publishing message to exchange with delay: {}", delayMilliseconds);
        val properties = getPropertiesWithDelay(delayMilliseconds);
        publishWithDelay(message, properties);
    }

    private void publishWithDelay(final Message message, final AMQP.BasicProperties properties) throws Exception {
        if (!config.isDelayed()) {
            log.warn("Publishing delayed message to non-delayed queue queue:{}", queueName);
        }
        if (config.getDelayType() == DelayType.TTL) {
            val routingKey = getRoutingKey(message);
            val context = PublishObserverContext.builder()
                    .queueName(queueName)
                    .messageProperties(properties)
                    .build();
            observer.executePublish(context, messageDetails -> {
                try {
                    publishChannel.basicPublish(ttlExchange(config),
                            routingKey, messageDetails.getMessageProperties(),
                            mapper().writeValueAsBytes(message));
                } catch (IOException e) {
                    log.error("Error while publishing", e);
                    throw RabbitmqActorException.propagate(e);
                }
                return null;
            });
        } else {
            publish(message, properties);
        }
    }
    public final void publishWithExpiry(final Message message, final long expiryInMs) throws Exception {
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .deliveryMode(2)
                .build();
        val finalProperties = getPropertiesWithExpiry(properties, expiryInMs);
        publish(message, finalProperties);
    }

    public final void publishWithDelayAndExpiry(final Message message,
                                                final long expiryInMs,
                                                final long delayMilliseconds) throws Exception {
        AMQP.BasicProperties properties = getPropertiesWithDelay(delayMilliseconds);
        val finalProperties = getPropertiesWithExpiry(properties, expiryInMs);
        publishWithDelay(message, finalProperties);

    }

    public final void publish(final Message message) throws Exception {
        publish(message, MessageProperties.MINIMAL_PERSISTENT_BASIC);
    }

    public final void publish(final Message message, final AMQP.BasicProperties properties) throws Exception {
        val routingKey = getRoutingKey(message);
        val context = PublishObserverContext.builder()
                .queueName(queueName)
                .messageProperties(properties)
                .build();
        observer.executePublish(context, messageDetails -> {
            val enrichedProperties = getEnrichedProperties(messageDetails.getMessageProperties());
            try {
                publishChannel.basicPublish(config.getExchange(), routingKey, enrichedProperties, mapper().writeValueAsBytes(message));
            } catch (IOException e) {
                log.error("Error while publishing", e);
                throw RabbitmqActorException.propagate(e);
            }
            return null;
        });
    }

    private AMQP.BasicProperties getEnrichedProperties(AMQP.BasicProperties properties) {
        HashMap<String, Object> enrichedHeaders = new HashMap<>();
        if (properties.getHeaders() != null) {
            enrichedHeaders.putAll(properties.getHeaders());
        }
        enrichedHeaders.put(Constants.MESSAGE_PUBLISHED_TEXT, Instant.now()
                .toEpochMilli());
        return properties.builder()
                .headers(Collections.unmodifiableMap(enrichedHeaders))
                .build();
    }

    private AMQP.BasicProperties getPropertiesWithDelay(final long delayMilliseconds){
        if(config.getDelayType() == DelayType.TTL){
            return new AMQP.BasicProperties.Builder()
                    .expiration(String.valueOf(delayMilliseconds))
                    .deliveryMode(2)
                    .build();
        }
            return new AMQP.BasicProperties.Builder()
                    .headers(Collections.singletonMap("x-delay", delayMilliseconds))
                    .deliveryMode(2)
                    .build();
    }

    private AMQP.BasicProperties getPropertiesWithExpiry(final AMQP.BasicProperties properties, final long expiryInMs){
        if (expiryInMs <= 0) {
            return properties;
        }
        val expiresAt = Instant.now()
                .toEpochMilli() + expiryInMs;
        return new AMQP.BasicProperties.Builder().headers(ImmutableMap.of(Constants.MESSAGE_EXPIRY_TEXT, expiresAt))
                .build();
    }

    public final long pendingMessagesCount() {
        try {
            if (config.isSharded()) {
                long messageCount  = 0 ;
                for (int i = 0; i < config.getShardCount(); i++) {
                    String shardedQueueName = NamingUtils.getShardedQueueName(queueName, i);
                    messageCount += publishChannel.messageCount(shardedQueueName);
                }
                return messageCount;
            }
            else {
                return publishChannel.messageCount(queueName);
            }
        } catch (IOException e) {
            log.error("Issue getting message count. Will return max", e);
        }
        return Long.MAX_VALUE;
    }

    public final long pendingSidelineMessagesCount() {
        try {
            return publishChannel.messageCount(NamingUtils.getSideline(queueName));
        } catch (IOException e) {
            log.error("Issue getting message count. Will return max", e);
        }
        return Long.MAX_VALUE;
    }

    public final long pendingSidelineProcessorMessagesCount() {
        try {
            return publishChannel.messageCount(NamingUtils.getSidelineProcessor(queueName));
        } catch (IOException e) {
            log.error("Issue getting message count. Will return max", e);
        }
        return Long.MAX_VALUE;
    }

    public void start() throws Exception {
        final String exchange = config.getExchange();
        final String dlx = NamingUtils.getSideline(config.getExchange());
        final ExchangeType exchangeType = getExchangeType();
        if (config.isDelayed()) {
            ensureDelayedExchange(exchange);
        } else {
            ensureExchange(exchange, exchangeType);
        }
        // Sideline/DLX is always DIRECT: dead-lettered messages route by exact routing key = queue name.
        // FANOUT would broadcast to all sideline queues; TOPIC's patterns wouldn't match the queue name.
        ensureExchange(dlx, ExchangeType.DIRECT);

        this.publishChannel = connection.newChannel();
        String sidelineQueueName = NamingUtils.getSideline(queueName);
        connection.ensure(sidelineQueueName, queueName, dlx, connection.rmqOpts(config));
        if (config.isSharded()) {
            // Sharding is only valid with a DIRECT exchange (TOPIC/FANOUT + sharding are rejected by
            // ActorConfig validation). So here mainExchangeBindingKeys(shardedQueueName) resolves via the
            // DIRECT visitor to a single binding key equal to the shard queue name - i.e. each shard queue
            // is bound point-to-point by its own name. The ensureWithBindingKeys call is shared with the
            // TOPIC/FANOUT path only for uniformity; it carries no pattern/broadcast semantics here.
            int bound = config.getShardCount();
            for (int shardId = 0; shardId < bound; shardId++) {
                String shardedQueueName = NamingUtils.getShardedQueueName(queueName, shardId);
                connection.ensureWithBindingKeys(shardedQueueName, config.getExchange(),
                        mainExchangeBindingKeys(shardedQueueName),
                        dlqOpts(dlx, shardedQueueName));
                connection.addBinding(sidelineQueueName, dlx, shardedQueueName);
            }
        } else {
            connection.ensureWithBindingKeys(queueName, config.getExchange(),
                    mainExchangeBindingKeys(queueName),
                    dlqOpts(dlx, queueName));
        }

        if (config.getDelayType() == DelayType.TTL) {
            connection.ensure(ttlQueue(queueName),
                    queueName,
                    ttlExchange(config),
                    connection.rmqOpts(exchange, config));
        }

        if (config.isSidelineProcessorEnabled()) {
            final var sidelineProcessorExchange = NamingUtils.getSidelineProcessor(config.getExchange());
            // Always DIRECT: the sideline-processor queue is bound by its exact queue name, so we need
            // point-to-point routing regardless of the main exchange type (TOPIC patterns wouldn't match
            // the queue name and FANOUT would broadcast to every sideline-processor queue).
            ensureExchange(sidelineProcessorExchange, ExchangeType.DIRECT);

            final var sidelineProcessorQueue = NamingUtils.getSidelineProcessor(queueName);

            if(config.isSharded())
            {
                int bound = config.getShardCount();

                for (int shardId = 0; shardId < bound ; shardId++)
                {
                    String shardedQueueName = NamingUtils.getShardedQueueName(sidelineProcessorQueue, shardId);
                    connection.ensure(shardedQueueName, sidelineProcessorExchange,
                            dlqOpts(dlx, shardedQueueName));
                    connection.addBinding(sidelineQueueName, dlx, shardedQueueName);
                }
            }
            else
            {
                connection.ensure(sidelineProcessorQueue, sidelineProcessorExchange,
                        dlqOpts(dlx, sidelineProcessorQueue));
                connection.addBinding(sidelineQueueName, dlx, sidelineProcessorQueue);
            }
        }
    }

    private ExchangeType getExchangeType() {
        final ExchangeType exchangeType = config.getExchangeType();
        // TOPIC routes on a message-derived key, so a resolver is mandatory.
        if (exchangeType == ExchangeType.TOPIC && routingKeyResolver == null) {
            throw new IllegalStateException(String.format(
                    "A RoutingKeyResolver must be supplied for TOPIC exchange. queue: %s", queueName));
        }
        // TTL delay re-routes via the queue name, which only works on a DIRECT exchange (see ensureDelayedExchange).
        if (config.isDelayed() && config.getDelayType() == DelayType.TTL
                && exchangeType != ExchangeType.DIRECT) {
            throw new IllegalStateException(String.format(
                    "TTL based delay is only supported for a DIRECT exchange. queue: %s", queueName));
        }
        return exchangeType;
    }

    private List<String> mainExchangeBindingKeys(final String queue) {
        return config.getExchangeType()
                .handleConfig(new ExchangeTypeBindingKeysVisitor(config, queue));
    }

    /**
     * Builds the queue arguments for a dead-lettered queue.
     *
     * <p>For a DIRECT exchange we deliberately do NOT set {@code x-dead-letter-routing-key}, preserving the
     * historical behaviour (RabbitMQ reuses the original routing key, which equals the queue name). This keeps
     * queue arguments byte-identical to previous versions and avoids PRECONDITION_FAILED errors when redeclaring
     * pre-existing queues on upgrade.
     *
     * <p>For TOPIC/FANOUT exchanges the original routing key would not match the direct {@code _SIDELINE} binding,
     * so we pin {@code x-dead-letter-routing-key} to the queue name to ensure dead-lettered messages reach the
     * correct sideline queue. These are new queues, so there is no existing-arg conflict.
     */
    private Map<String, Object> dlqOpts(final String deadLetterExchange,
                                        final String deadLetterRoutingKey) {
        if (config.getExchangeType() == ExchangeType.DIRECT) {
            return connection.rmqOpts(deadLetterExchange, config);
        }
        return connection.rmqOpts(deadLetterExchange, deadLetterRoutingKey, config);
    }

    private void ensureExchange(String exchange, ExchangeType exchangeType) throws IOException {
        connection.channel().exchangeDeclare(
                exchange,
                exchangeType.amqpType(), true);
        log.info("Created exchange: {} of type: {}", exchange, exchangeType.amqpType());
    }

    private void ensureDelayedExchange(String exchange) throws IOException {
        if (config.getDelayType() == DelayType.TTL) {
            // TTL delay is guarded to DIRECT only (see getExchangeType), so both exchanges are DIRECT.
            ensureExchange(ttlExchange(config), ExchangeType.DIRECT);
            ensureExchange(exchange, ExchangeType.DIRECT);
        } else {
            // https://blog.rabbitmq.com/posts/2015/04/scheduling-messages-with-rabbitmq/
            connection.channel().exchangeDeclare(
                    exchange,
                    "x-delayed-message",
                    true,
                    false,
                    ImmutableMap.<String, Object>builder()
                            // Delayed exchange forwards using the configured type once the delay elapses.
                            .put("x-delayed-type", config.getExchangeType().amqpType())
                            .build());
            log.info("Created delayed exchange: {}", exchange);
        }
    }

    private String ttlExchange(ActorConfig actorConfig) {
        return String.format("%s_TTL", actorConfig.getExchange());
    }

    private String ttlQueue(String queueName) {
        return String.format("%s_TTL", queueName);
    }

    public void stop() throws Exception {
        try {
            if (publishChannel.isOpen()) {
                publishChannel.close();
                log.info("Publisher channel closed for queue [{}]", queueName);
            } else {
                log.warn("Publisher channel already closed for queue [{}]", queueName);
            }
        } catch (Exception e) {
            log.error("Error closing publisher channel for queue [{}]", queueName, e);
            throw e;
        }
    }

    protected final RMQConnection connection() {
        return connection;
    }

    protected final ObjectMapper mapper() {
        return mapper;
    }

    private String getRoutingKey(Message message) {
        // Routing key depends on exchange type: DIRECT -> queue/shard name, TOPIC -> resolver, FANOUT -> ignored.
        return config.getExchangeType().handleConfig(new ExchangeTypeVisitor<String>() {
            @Override
            public String visitDirect() {
                return config.isSharded()
                       ? NamingUtils.getShardedQueueName(queueName, shardIdCalculator.calculateShardId(message))
                       : queueName;
            }

            @Override
            public String visitTopic() {
                return routingKeyResolver.resolve(message);
            }

            @Override
            public String visitFanout() {
                return "";
            }
        });
    }

}
