# advanced/04 — portability and capabilities

A library spanning several brokers has two dishonest options and one honest one.

It can expose the **lowest common denominator**, which throws away the reason
people chose RabbitMQ. It can **pretend everything is supported** and fail at run
time in production. Or it can **say what each transport can do** and let the
application decide.

## What it demonstrates

The same code, two transports, in one run:

```
  === memory://orders ===
  supports   4 of 13 capabilities
  missing    [CONSISTENT_HASH_ROUTING, DELAYED_DELIVERY, HEADERS_ROUTING, PRIORITY,
              QUORUM_QUEUES, SINGLE_ACTIVE_CONSUMER, STREAMS, TRANSACTIONS, TTL_PER_MESSAGE]
  queue      classic — this transport has no quorum queues,
             so durability is whatever the single node gives us
  streams    refused, and says which transport and why

  === amqp://localhost ===
  supports   10 of 13 capabilities
  missing    [CONSISTENT_HASH_ROUTING, DELAYED_DELIVERY, TRANSACTIONS]
  queue      quorum, replicated
  streams    available
```

Nothing in the example branches on *which* transport it is. It branches on what
the transport says it can do — the difference between portable code and code with
an `if (rabbitmq)` in it.

## The shape that ports

```java
if (mq.supports(Capability.QUORUM_QUEUES)) {
    mq.declareQueue(queue);                                // replicated
} else {
    mq.declareQueue(queue, QueueType.CLASSIC, Map.of());   // and say so
}
```

The feature is used where it exists and a documented alternative where it does
not. When a third transport arrives, this code does not change.

The `else` branch is not a silent fallback — it prints what it settled for.
A degradation nobody is told about is a degradation somebody will be surprised
by during an incident.

## Skipping the check fails early, on purpose

```
  streams    refused, and says which transport and why
```

Declaring a stream on a transport without them **throws**, naming the transport
and the queue. It does not quietly give you a classic queue that behaves
differently — which would pass every test and lose the replay, the retention and
the independent consumer positions that were the reason for asking.

That failure lands at declare time, on a laptop, rather than at three in the
morning.

## A capability describes the library, not the broker

`TRANSACTIONS` is in RabbitMQ's *missing* list, and RabbitMQ has transactions.
That is deliberate: this library offers no way to reach `tx.select`, so claiming
the capability would mean `supports(TRANSACTIONS)` returning true with nothing to
call.

It was claimed until `0.2.6`, along with `PRIORITY` and
`SINGLE_ACTIVE_CONSUMER`, none of which had any API. Writing *this example* is
what made it obvious — it tells readers to branch on `supports(...)`, and for
three capabilities there was nothing to branch to. `PRIORITY` gained
`PublishOptions.withPriority(int)`; `SINGLE_ACTIVE_CONSUMER` was already
reachable as a queue argument; `TRANSACTIONS` stopped being claimed.

## Why the in-memory transport claims so little

Four of thirteen, and that is deliberate. It implements exchange routing, topic
wildcards, publisher confirms and dead-lettering — enough to test most
application logic — and claims **nothing else**. Code depending on quorum queues
or streams therefore fails against it exactly as it would against a broker that
lacks them, instead of passing in tests and failing in production. See
[intermediate/04](../../intermediate/04-testing-without-a-broker).

## Running it

```bash
mvn compile exec:java                          # in-memory only
docker compose up -d
mvn compile exec:java -Dexec.args=amqp://localhost   # both, side by side
```

## Related

- [intermediate/04](../../intermediate/04-testing-without-a-broker) — what the in-memory transport refuses to fake
- [Architecture](https://acemq-company.github.io/acemq-java-amqp/)
