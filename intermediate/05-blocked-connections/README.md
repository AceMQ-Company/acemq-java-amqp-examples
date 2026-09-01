# intermediate/05 — blocked connections

RabbitMQ raises an alarm when memory or disk crosses a watermark and **stops
accepting publishes**. It does not close the connection. It does not fail
anything already sent. Publishers simply stop being served.

That is a deliberate design — stopping producers is what lets consumers drain the
backlog — and it is also why the failure is confusing the first time. Nothing is
broken. Nothing is closed. Publishes just do not complete.

## What it demonstrates

- **The publish is refused, and the exception says why.** `reason()` carries the
  broker's own words: `low on disk`, not `publish failed`.
- **`mayHaveBeenPublished()`** decides whether a retry is safe or a duplicate
  risk.
- **Consumers keep working.** The queue is readable throughout.
- **Recovery needs no reconnect.** Once the alarm clears, publishing resumes on
  the same connection.

## How long a blocked publish waits

`blockedTimeout` decides it. **The default is thirty seconds**, which is a
sensible bet that a brief alarm will pass — and is also thirty seconds of a
request thread doing nothing, per message, for as long as the alarm lasts.

```java
AceMq.connect(ConnectionConfig.url("amqps://broker:5671")
        .blockedTimeout(Duration.ofSeconds(2))
        .build());
```

Choose it deliberately. "The whole thread pool is parked on publish" is how one
broker low on disk takes an application down with it. A short timeout turns that
into a fast failure you can shed load on; a long one turns it into an outage.

The example sets two seconds, and prints how long the call actually held the
thread.

## The part that catches people

**An idle connection finds out nothing.** A publisher discovers the alarm by
publishing. A service that publishes once a minute learns a minute late; one
that is idle does not learn at all until it next has something to send.

So this is not a condition you can wait for an event about and then act on. The
practical shape is: catch `ConnectionBlockedException` where you publish, and
treat `isBlocked()` as something to *report*, not something to poll and
pre-emptively branch on.

This library's own integration testing against RabbitMQ is where that was pinned
down — the assumption going in was that the broker announces an alarm to every
connected client, and a probe against a real broker showed the block surfacing at
the next publish instead. The example runs against the in-memory transport, which
reports `isBlocked()` as soon as the alarm is set; the exception behaviour is the
same either way, and that is the part your code has to handle.

## Do not shut the consumers down

The instinct on a publish failure is to stop the service. Here that is the wrong
move: consumers draining the queues are the only thing that will clear the alarm.
Stop publishing, keep consuming, alert on the reason.

## Why no container

Reproducing a real alarm means filling a disk. The in-memory transport can be
told to block on demand:

```java
InMemoryTransport.block("orders", "low on disk");
InMemoryTransport.unblock("orders");
```

The host part of the `memory://` URL names the broker, which is what `block`
takes.

## Running it

```bash
mvn compile exec:java      # no broker required
```

## What to expect

```
  before     published, blocked=false
  alarm      raised, isBlocked=true reason=low on disk
  during     refused: reason=low on disk
  during     mayHaveBeenPublished=false
  during     the publish waited about 2 s
  meanwhile  the queue still holds 1, consumers unaffected
  after      cleared, published again, blocked=false
  queue      holds 2
```

## Related

- [Reliability](https://acemq-company.github.io/acemq-java-amqp/reliability.html)
- [Testing](https://acemq-company.github.io/acemq-java-amqp/testing.html)
