package io.appform.dropwizard.actors.messaging;

import java.util.Set;

/**
 * Consumes events from one queue. Handlers have no way to publish to the exchange they consume from; use
 * {@link MessageContext#retryLater} to redeliver an event to this queue only.
 */
@FunctionalInterface
public interface QueueHandler<E> {

    /**
     * @return true to ack the event; false to reject it (it is dead-lettered to the queue's sideline queue).
     *         Exceptions are retried according to the queue's {@code retryConfig}.
     */
    boolean handle(E event, MessageContext<E> context) throws Exception;

    /**
     * Called instead of {@link #handle} for events published with an expiry that has passed. Acks by default.
     */
    default boolean handleExpired(final E event, final MessageContext<E> context) throws Exception {
        return true;
    }

    /**
     * Exceptions (and subtypes, anywhere in the cause chain) for which the event is acked and dropped instead of
     * rejected.
     */
    default Set<Class<? extends Throwable>> droppedExceptionTypes() {
        return Set.of();
    }
}
