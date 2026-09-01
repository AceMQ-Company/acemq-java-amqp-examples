# intermediate/09 — shared idempotency

[basic/05](../../basic/05-idempotent-consumer) makes **one instance** idempotent
with an in-memory store. Run four of them and the guarantee is gone: each has its
own memory, so a duplicate redelivered to a different instance is handled again.

That failure appears only after you scale up, and only under load. A shared store
is the answer, and the interesting part is that a claim is a **lease**, not a
lock.

## What it demonstrates

- **Two instances, one message, one winner.** `claim()` returns `false` for the
  loser.
- **Still refused later**, which is what retention buys: a redelivery weeks after
  both instances have been replaced is recognised.
- **An abandoned claim expires.** An instance that claims a message and dies never
  confirms and never releases; the claim times out and another instance takes it.
- **`release()`** hands a message back immediately, for a handler that failed and
  wants it retried now.

## Why a lease and not a lock

A lock held by a process that has been OOM-killed is held forever. The message
becomes permanently unhandleable and the only cure is somebody deleting a row by
hand at three in the morning.

A lease expires. At-least-once survives: the work happens **late** rather than
never.

The cost is the obvious one — if the handler is slower than the claim timeout,
two instances can both be working on the same message. So:

**`claimTimeout` must be comfortably longer than the slowest handler.**

| | |
|---|---|
| Too short | Duplicate work, which is exactly what the store was for |
| Too long | A message stuck for that long after an instance dies |

Minutes is usually right. The example uses two seconds so it finishes.

## Retention is a different clock

`retention` is how long a *confirmed* message id is remembered, and it has to
outlive the longest redelivery you can imagine — including a
[replay](../../basic/04-replay) run months later. Rows are not free, so this is a
real trade: `purgeExpired()` exists to be run on a schedule.

## The window that matters

Between `claim()` and `confirm()` is the handler's duration, and that is the only
window where a crash costs you anything. Confirm as soon as the work is durable,
and — if the work is a database write — **in the same transaction**, which makes
the pair genuinely exactly-once rather than nearly.

## Running it

```bash
mvn compile exec:java      # H2 in memory; no broker, no Docker
```

## What to expect

```
  claim      A=true B=false
  confirmed  isConfirmed=true
  again      claim=false
  orphan     claimed by A=true
  orphan     B cannot take it yet=true
  orphan     after the lease expires, B claims it=true
  released   another instance can claim at once=true
  rows       3
```

## Related

- [basic/05](../../basic/05-idempotent-consumer) — the single-instance version
- [Reliability](https://acemq-company.github.io/acemq-java-amqp/reliability.html)
