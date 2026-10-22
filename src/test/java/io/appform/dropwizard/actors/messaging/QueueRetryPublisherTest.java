package io.appform.dropwizard.actors.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;
import io.appform.dropwizard.actors.common.Constants;
import io.appform.dropwizard.actors.common.RabbitmqActorException;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class QueueRetryPublisherTest {

    private Channel channel;
    private RMQConnection connection;

    @BeforeEach
    void setUp() throws Exception {
        channel = mock(Channel.class);
        when(channel.isOpen()).thenReturn(true);
        connection = mock(RMQConnection.class);
        when(connection.newChannel()).thenReturn(channel);
    }

    private QueueRetryPublisher started(final boolean confirms) throws Exception {
        final QueueRetryPublisher publisher = new QueueRetryPublisher("ns.p.Q", connection, new ObjectMapper(),
                confirms, 1_000);
        publisher.start();
        return publisher;
    }

    @Test
    void publishesToPrivateRetryExchangeWithDelayAndAppHeadersOnly() throws Exception {
        started(true).publish("m", Map.of("app", "v", "x-death", "d", "x-delay", 1L), Duration.ofSeconds(3));

        final ArgumentCaptor<AMQP.BasicProperties> props = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
        verify(channel).basicPublish(eq("ns.p.Q_RETRY"), eq("ns.p.Q"), props.capture(), any());
        verify(channel).waitForConfirmsOrDie(1_000);
        final Map<String, Object> headers = props.getValue().getHeaders();
        assertEquals("v", headers.get("app"));
        assertFalse(headers.containsKey("x-death"));
        assertEquals(3_000L, headers.get("x-delay"));
        assertTrue(headers.containsKey(Constants.MESSAGE_PUBLISHED_TEXT));
        assertEquals(2, props.getValue().getDeliveryMode());
    }

    @Test
    void withoutConfirmsDoesNotWait() throws Exception {
        started(false).publish("m", null, Duration.ZERO);
        verify(channel, never()).confirmSelect();
        verify(channel, never()).waitForConfirmsOrDie(anyLong());
    }

    @Test
    void validatesDelayAndState() throws Exception {
        final QueueRetryPublisher publisher = new QueueRetryPublisher("ns.p.Q", connection, new ObjectMapper(),
                true, 1_000);
        assertThrows(IllegalStateException.class, () -> publisher.publish("m", null, Duration.ZERO));
        publisher.start();
        assertThrows(IllegalArgumentException.class, () -> publisher.publish("m", null, Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> publisher.publish("m", null, null));
        publisher.stop();
        verify(channel).close();
        assertThrows(IllegalStateException.class, () -> publisher.publish("m", null, Duration.ZERO));
    }

    @Test
    void confirmTimeoutSurfacesAsActorException() throws Exception {
        doThrow(new TimeoutException()).when(channel).waitForConfirmsOrDie(anyLong());
        assertThrows(RabbitmqActorException.class, () -> started(true).publish("m", null, Duration.ZERO));
    }

    @Test
    void reopensChannelClosedByChannelError() throws Exception {
        final QueueRetryPublisher publisher = started(true);
        when(channel.isOpen()).thenReturn(false);
        when(channel.getCloseReason()).thenReturn(new ShutdownSignalException(false, false, null, channel));
        final Channel replacement = mock(Channel.class);
        when(replacement.isOpen()).thenReturn(true);
        when(connection.newChannel()).thenReturn(replacement);

        publisher.publish("m", null, Duration.ZERO);

        verify(channel).abort();
        verify(replacement).basicPublish(eq("ns.p.Q_RETRY"), anyString(), any(), any());
    }

    @Test
    void failsFastWhileConnectionRecovers() throws Exception {
        final QueueRetryPublisher publisher = started(true);
        when(channel.isOpen()).thenReturn(false);
        when(channel.getCloseReason()).thenReturn(new ShutdownSignalException(true, false, null, null));

        assertThrows(RabbitmqActorException.class, () -> publisher.publish("m", null, Duration.ZERO));
        verify(channel, never()).abort();
    }
}
