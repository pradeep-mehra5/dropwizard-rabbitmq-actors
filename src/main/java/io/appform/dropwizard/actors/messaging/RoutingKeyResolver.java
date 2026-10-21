package io.appform.dropwizard.actors.messaging;

/**
 * Derives the routing key for an event published to a DIRECT or TOPIC exchange. Not used for FANOUT.
 */
@FunctionalInterface
public interface RoutingKeyResolver<E> {
    String resolve(E event);
}
