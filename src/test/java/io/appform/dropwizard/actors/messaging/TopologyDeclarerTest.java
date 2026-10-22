package io.appform.dropwizard.actors.messaging;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.Channel;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.messaging.MessagingNames.ExchangeNames;
import io.appform.dropwizard.actors.messaging.config.ExchangeSpec;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * The declaration channel must outlive {@code declare()}: the client's automatic topology recovery redeclares each
 * recorded entity on the channel it was declared on, so closing it makes every redeclare fail after a reconnect.
 */
class TopologyDeclarerTest {

    private static final ExchangeNames NAMES = new ExchangeNames("ex", "ex", "ex_SIDELINE");
    private static final ExchangeSpec SPEC = ExchangeSpec.builder().type(ExchangeType.DIRECT).build();

    @Test
    void keepsDeclarationChannelOpenUntilClosed() throws Exception {
        final Channel channel = mock(Channel.class);
        when(channel.isOpen()).thenReturn(true);
        final RMQConnection connection = mock(RMQConnection.class);
        when(connection.newChannel()).thenReturn(channel);
        final TopologyDeclarer declarer = new TopologyDeclarer("p", mock(BindingGuard.class));

        declarer.declare(NAMES, SPEC, connection);
        verify(channel, never()).close();

        declarer.close();
        verify(channel).close();
    }

    @Test
    void closesChannelWhenDeclarationFails() throws Exception {
        final Channel channel = mock(Channel.class);
        when(channel.isOpen()).thenReturn(true);
        doThrow(new IOException("406")).when(channel).exchangeDeclare(anyString(), anyString(), anyBoolean());
        final RMQConnection connection = mock(RMQConnection.class);
        when(connection.newChannel()).thenReturn(channel);
        final TopologyDeclarer declarer = new TopologyDeclarer("p", mock(BindingGuard.class));

        assertThrows(IOException.class, () -> declarer.declare(NAMES, SPEC, connection));
        verify(channel).close();

        declarer.close();
        verify(channel).close(); // not closed a second time
    }
}
