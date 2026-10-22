# Dropwizard RabbitMQ Actors Bundle
Provides actor abstraction on RabbitMQ for dropwizard based projects.

## Dependency

```
<dependency>
    <groupId>io.appform.dropwizard.actors</groupId>
    <artifactId>dropwizard-rabbitmq-actors</artifactId>
    <version>2.0.28-18</version>
</dependency>
```

## License
Apache-2.0

## Actors vs messaging

* **Actors** (`Actor`, `BaseActor`): point-to-point. One DIRECT exchange, one queue, one handler.
  `actor.publish(msg)` always reaches exactly that actor's queue.
* **Messaging** (`io.appform.dropwizard.actors.messaging`): publish/subscribe over DIRECT, TOPIC and FANOUT
  exchanges. Code publishes an event *to an exchange* through an `EventPublisher`; the configured queue bindings
  alone decide which queues receive it. Each queue is consumed by a `QueueHandler`, which has no way to publish to
  the exchange it consumes from.

A queue belongs either to an actor or to messaging, never both. Messaging queues use their own prefix
(`rabbitmq.messaging` by default), so they don't collide with actor queues by default. Don't reuse an actor's exchange
or queue name in the messaging config.

Set a `prefix` per service (e.g. `myservice.messaging`). With the shared default, two services configuring the same
queue name would share one broker queue: their consumers would compete for its events, and with different exchanges
the queue would be bound to both.

Exchange names in the config are logical names used in code. On the broker, exchanges and queues are prefixed with
the namespace (`NAMESPACE_ENV_NAME` / `rmq.actors.namespace`) when one is set, so feature environments sharing a
broker don't receive each other's events.

### Messaging config

```yaml
messaging:
  prefix: myservice.messaging          # default: rabbitmq.messaging; set one per service
  exchanges:
    order_events:
      type: TOPIC                      # DIRECT | TOPIC | FANOUT
      delayed: false                   # x-delayed-message exchange, enables publishWithDelay (see the risk below)
      captureUnroutable: true          # unmatched events go to the <prefix>.<exchange>_UNROUTED queue
      publisherConfirms: true          # default; publish waits for the broker's confirm
      queues:
        shipping:
          bindingKeys: ["order.*.PLACED"]
          concurrency: 5
        audit:
          bindingKeys: ["order.#"]
          retryLaterEnabled: true      # MessageContext.retryLater redelivers to this queue only
```

Optionally enable the binding drift checks through the Management API on the RabbitMQ config:

```yaml
managementConfig:
  enabled: true
  port: 15672
  secure: false                        # https uses the JVM's default trust store
```

The Management API is called on the **first** broker, with the AMQP credentials. That user needs a management tag
(e.g. `monitoring`). If the API can't be read while the checks are enabled, startup fails.

### Usage

Enable messaging by overriding `RabbitmqActorBundle.getMessagingConfig`, then register publishers and handlers
before the application starts:

```java
Messaging messaging = bundle.getMessaging();

EventPublisher<OrderEvent> publisher = messaging.publisher("order_events", OrderEvent::routingKey);

messaging.consumer("order_events", "shipping", OrderEvent.class, (event, context) -> ship(event));
messaging.consumer("order_events", "audit", OrderEvent.class, (event, context) -> {
    if (!ready(event)) {
        context.retryLater(event, Duration.ofMinutes(1));   // this queue only
        return true;
    }
    return audit(event);
});

publisher.publish(event);
```

FANOUT exchanges take no routing key: `messaging.publisher("exchange")`.

Each `publisher(...)` call opens its own channel: register one publisher per exchange (or per event type) and reuse it.
`bundle.getMessaging()` throws if messaging is not configured.

When using `retryLater`, call it as the last step and then `return true`. Calling it and then throwing or returning
`false` produces duplicates (the handler is retried, or the original is also sidelined). Handlers should be idempotent.

Events are deserialized with Dropwizard's `ObjectMapper`. To let publishers add fields before consumers are updated,
make event classes tolerate unknown properties (`@JsonIgnoreProperties(ignoreUnknown = true)`).

### Rules and guarantees
* **Startup validation.** Startup fails if:
  * no exchange is configured, or an exchange has no queues. Publishing to an exchange without consuming from it in
    the same service is not supported;
  * a DIRECT/TOPIC queue has no binding keys, a FANOUT queue has binding keys, or a binding key is blank, has
    surrounding whitespace or (TOPIC) an empty word;
  * a queue name ends with `_SIDELINE`, `_RETRY` or `_UNROUTED`, or an exchange name ends with `_SIDELINE` or
    `_UNROUTED` (reserved for derived names);
  * a configured queue has no handler;
  * a publisher or handler refers to an unknown exchange or queue.
* **Topology first.** All exchanges, queues and bindings are declared before any publisher or consumer starts.
* **No silent loss.** Publishes use `mandatory`. With publisher confirms (the default), an event no binding matches
  fails with `UnroutableMessageException`, and a publish the broker did not accept fails too. With
  `captureUnroutable`, unmatched events are parked in the `<prefix>.<exchange>_UNROUTED` queue instead.
  * Confirms cost a broker round trip per publish, and publishes on one exchange are serialized. Set
    `publisherConfirms: false` for fire-and-forget throughput; unroutable events are then only logged.
  * A confirm timeout can leave the caller unsure whether the broker got the event, so retries can duplicate it.
    Consumers should be idempotent.
  * **Risk with delayed exchanges:** the delayed-message plugin does not support `mandatory`, so an event that no
    binding matches is *not* detected, even with confirms, and is dropped silently. Set `captureUnroutable: true` on
    delayed exchanges to keep such events.
  * A publisher whose channel gets closed (confirm timeout, broker error) reopens it on the next publish.
  * Routing keys are limited to 255 bytes.
* **Retries stay local.** `retryLater` goes through a private per-queue delayed exchange (`<queue>_RETRY`), so a
  retry never reaches the other queues of the exchange.
* **Sideline.** Each queue dead-letters rejected events to its own `<queue>_SIDELINE` queue.
* **Consumption** runs on the same consumer as actors: concurrency, prefetch, retry config, exception handling,
  expiry (`publishWithExpiry` / `handleExpired`) and metrics behave identically.
* **Queue arguments** (queue type, lazy mode, quorum group size, auto-delete `ttlConfig`) are built as for actors.
  `ttlConfig` deletes an unused queue (`x-expires`); it is not a message TTL. Priority queues are not supported.
  `haMode`/`haParams` are not supported either: as queue arguments they don't enable mirroring. For replicated queues
  use `queueType: QUORUM`, or a mirroring policy matching the messaging queue names.

### Changing topology
* Adding queues or exchanges is always safe.
* A queue's binding keys and exchange are fixed once it exists. With `managementConfig.enabled`, changing them
  fails startup; without it, old bindings silently remain. Use a new queue instead.
* Queue arguments (queue type, lazy mode, TTL) and exchange settings (`type`, `delayed`, `captureUnroutable`)
  can't be changed on an existing queue/exchange (the broker rejects the redeclare with `406 PRECONDITION_FAILED`).
* Removing a queue from the config does not delete it on the broker; it stays bound and keeps collecting events.
  Delete or unbind it manually.
* The `<prefix>.<exchange>_UNROUTED` queue has no auto-delete TTL, even when the exchange's queues set `ttlConfig`.
  It is not removed when idle (e.g. in feature environments); delete it manually.

### Not supported (yet)
These actor features have no messaging equivalent; use an actor if you need them.
* **Sharding** (`shardCount`): a messaging queue is a single broker queue, scaled with `concurrency`, so ordering is
  not guaranteed across consumers.
* **Sideline processor** (`sidelineProcessorConfig`): rejected events stay in `<queue>_SIDELINE` until moved back
  manually; nothing reprocesses them automatically.
* **TTL-based delay** (`DelayType.TTL`): delays need the delayed-message plugin (`delayed: true`).
* **Per-message priority** and priority queues.
* **Publishing with both a delay and an expiry** (actors' `publishWithDelayAndExpiry`).
* **Publishing to an exchange without consuming from it** in the same service.

### Requirements
`delayed` and `retryLaterEnabled` need the `rabbitmq_delayed_message_exchange` plugin. The drift checks need the
`rabbitmq_management` plugin.

# NOTE
Package and group id has changed from `io.dropwizard.actors` to `io.appfrom.dropwizard.actors` from 1.3.12-1.
