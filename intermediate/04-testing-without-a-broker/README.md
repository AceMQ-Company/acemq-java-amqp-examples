# intermediate/04 — testing without a broker

Every other example in this repository starts a container. This one does not,
and neither should most of your tests.

`memory://orders` connects to an in-process broker. Publishing, consuming,
routing, acknowledgement and the failure policies behave as they do over AMQP,
and the tests run in **0.29 seconds** rather than waiting on Docker.

## What it demonstrates

- **A unit test of messaging code.** `OrderEscalationTest` is a plain JUnit test —
  surefire, no `@Testcontainers`, no `IT` suffix. It runs on every save.
- **The host names the broker.** Two URLs with different hosts are two separate
  brokers, which is how a suite runs cases in parallel without them seeing each
  other's queues.
- **`reset()` discards everything**, so one test cannot leave state that makes
  the next one pass.
- **It refuses what it cannot honestly provide** rather than faking it.

## This is not a replacement for the integration tests

It is what makes the other ninety per cent of your tests fast enough to run
constantly. The integration tests prove the library works against RabbitMQ; tests
like `OrderEscalationTest` prove *your rules* are right, and those have no reason
to wait on a container.

Where the line falls: anything whose behaviour is the broker's — retry ladders
with real TTLs, quorum queues, streams, TLS, connection recovery — belongs in an
integration test. Anything whose behaviour is yours does not.

## Two things it will not pretend

```
  streams    refused: the in-memory transport does not support streams, so 'orders.log' cannot be one
```

Streams are not supported, and rather than quietly behaving like a queue — which
would pass a test and fail in production — the transport says so. The same is
true of **quorum queues**, and that one has a practical consequence:

```java
mq.declareQueue("orders.large");                              // asks for QUORUM, refused here
mq.declareQueue("orders.large", QueueType.CLASSIC, Map.of()); // what this example does
```

`declareQueue(name)` asks for a quorum queue, which is the right default against
RabbitMQ. Code you intend to test in memory needs the queue type to be a
parameter rather than a hard-coded default.

## reset() removes, it does not empty

```
  after reset the queue is gone: queue 'orders.large' does not exist
```

Worth knowing if your tests declare topology in a `@BeforeEach` — after a reset
there is nothing there at all, not an empty queue.

Call it in `@AfterEach`, not `@BeforeEach`. A test that leaves state behind
should fail the next one loudly rather than being cleaned up on the way in, where
the failure depends on execution order.

## A Maven trap, met here

The integration test is called `RunsWithoutABrokerIT`, not
`TestingWithoutABrokerIT`. Surefire's default includes are `Test*.java` **as well
as** `*Test.java`, so a class beginning with "Testing" is collected as a unit
test and runs twice — once in surefire, once in failsafe. Harmless for this one;
expensive for anything that starts a container.

## Running it

```bash
mvn compile exec:java      # no docker compose line, on purpose
```

## What to expect

```
  escalated  [large]
  after reset the queue is gone: queue 'orders.large' does not exist
  streams    refused: the in-memory transport does not support streams, so 'orders.log' cannot be one
  took       169 ms, no container started
```

## Related

- [Testing](https://acemq-company.github.io/acemq-java-amqp/testing.html)
