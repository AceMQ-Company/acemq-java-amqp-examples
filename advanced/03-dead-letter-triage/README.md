# advanced/03 — dead-letter triage

[basic/04](../../basic/04-replay) replays a queue where everything failed for the
same reason. Real dead-letter queues are mixed: a downstream that was briefly
down, a handful of genuinely malformed orders, and something nobody has looked at
yet.

Replaying all of it because one cause is fixed puts the broken ones straight back
through the ladder.

## What it demonstrates

- **Selective replay.** `replay(max, filter)` moves what matches and leaves the
  rest.
- **Two destinations, not one.** A message that failed goes to `.dlq`; a payload
  that will never decode goes to `.parked` and is never part of a bulk replay.
- **What is left is a decision**, not a backlog: two messages nobody should
  replay until somebody has read them.

```
  dead       4 in orders.new.dlq
  replayed   2, passed over the ones that did not match
  handled    [o-1, o-2]
  remaining  2 for a person to look at
  parked     1 that will never decode
```

## The filter is a drain, not a query

This is the part worth understanding before you rely on it:

> A message the filter rejects is **held, and requeued when the drain ends.**

AMQP has no way to look past a message without taking it. So the replay reads
the queue from the head, moves what the filter accepts, and keeps each message it
passes over unacknowledged until the queue is empty or `max` have moved. Then it
requeues every held message, in the order they arrived, and waits until they are
back before it returns: `pending()` straight afterwards is the true number.

Consequences to plan around:

- A filter that rejects the first message still moves the later matches. In this
  example an `XXX` order sits at the head; both timeouts behind it move anyway.
- A filter that matches little holds up to the whole queue unacknowledged at
  once. On a queue with forty thousand messages, bound it with `max` or replay
  in stages.
- An explicit requeue does not count towards a quorum queue's delivery limit, so
  passing a message over does not push it closer to being dropped.

Before 0.7.16 the Java library stopped at the first rejected message instead, so
this example moved nothing until the `XXX` at the head was dealt with. All five
AceMQ libraries now behave the same way.

## `.dlq` and `.parked` are different problems

| | Means | Fix |
|---|---|---|
| `orders.new.dlq` | The handler ran and failed, out of attempts | Fix the cause, replay |
| `orders.new.parked` | The payload never decoded | No redeployment helps; a person, or a schema |

Parking is why a bulk `replayAll()` is safe to run: the messages that could never
succeed are not in that queue to begin with.

An `AceFatalException` from a handler says "do not retry this" — which is how the
`XXX` orders reach the dead-letter queue immediately rather than after the ladder.

## Before you replay anything

```java
Replay replay = mq.replay("orders.new");
replay.pending();     // how many, before deciding
```

Replaying forty thousand messages into a consumer that is already behind is a
decision, and that is the number the decision needs. Then a bounded trial —
`replay(1)` — before you believe the fix.

## Running it

```bash
mvn compile exec:java      # no broker required
```

## Related

- [basic/04](../../basic/04-replay) — replay basics and provenance
- [basic/03](../../basic/03-retries-and-dead-letters) — how messages get there
- [Reliability](https://acemq-company.github.io/acemq-java-amqp/reliability.html)
