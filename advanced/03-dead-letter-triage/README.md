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
  replayed   2, stopped at the first message that did not match
  handled    [o-1, o-2]
  remaining  2 for a person to look at
  parked     1 that will never decode
```

## The filter is a drain, not a query

This is the part worth understanding before you rely on it:

> A message the filter rejects is **requeued, and the drain stops there.**

AMQP has no way to look past a message without taking it. Skipping one means
holding it while reading the next, and a drain that held every rejected message
would pull the queue's worth of unsettled deliveries into memory.

So the filter reads from the **head** and halts at the first message it does not
want. Consequences to plan around:

- If the first message does not match, **nothing moves.** That is visible, which
  is the point — the alternative is quietly reordering the queue.
- Replaying "all the timeouts" from a queue that starts with a malformed order
  moves nothing until that order is dealt with.
- Run it repeatedly rather than expecting one pass to sort the queue.

In this example the two timeouts happen to be at the head, so both move and the
drain stops on the first `XXX`. Had they been interleaved, it would have moved
one and stopped.

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
