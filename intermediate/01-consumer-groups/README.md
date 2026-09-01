# intermediate/01 — consumer groups

Two numbers decide how a queue behaves under load, and they get confused
constantly.

**Concurrency** is how many handlers run at once. **Prefetch** is how many
messages the broker may hand one consumer before it acknowledges any of them.
Getting the second one wrong is what makes a queue with eight consumers behave
like a queue with one.

## What it demonstrates

- **Scaling a live group.** `scaleTo(8)` adds consumers to a running group on the
  same connection. Nothing is redeclared and nothing in flight is disturbed.
- **The measurement that means something.** 40 messages at 50ms each take ~2,400ms
  one at a time and ~300ms eight at a time — roughly 8×.
- **`drain(Duration)` before closing.** Stop taking new messages, let the ones in
  progress finish.
- **Nothing lost or duplicated** across the scale-up: 80 handled, 80 sent.

## Prefetch is the setting people get wrong

With `prefetch(1)` a consumer is given one message and gets another only when it
has finished. Work spreads evenly.

Raise it and the broker hands a **batch** to whoever asks first. One consumer
sits on thirty messages, working through them at 50ms apiece, while seven others
have nothing to do and the queue depth does not move. **A queue with idle
consumers and a growing backlog is almost always a prefetch problem, not a
scaling problem.**

The trade is throughput against fairness:

| Prefetch | Use when |
|---|---|
| 1 | Handlers are slow or uneven. Fair distribution matters more than round trips. |
| 10–50 | Handlers are fast and similar. Round-trip cost starts to dominate. |
| Unbounded | Never. One consumer takes the queue and memory grows without limit. |

## Concurrency is measured, not counted

The example counts **handlers running at the same moment**, not thread names.
That distinction cost a test here: with one consumer, the run still touched 16
distinct threads, because the group dispatches onto a pool and a handler runs on
whichever thread is free. Counting threads would have reported 16-way
concurrency during a strictly serial run.

## Running it

```bash
docker compose up -d
mvn compile exec:java
```

## What to expect

```
  group      size=1 prefetch=1
  1 consumer  40 messages in 2365 ms, peak concurrency 1
  scaled to  size=8
  8 consumers 40 messages in 302 ms, peak concurrency 8
  speedup    7.8x
  drained    true, inFlight=0 acknowledged=80 rejected=0
  handled    80 of 80, each exactly once
```

## Draining is not optional

Closing a group without draining acknowledges nothing that was in flight. Those
messages are redelivered to whoever connects next — correct, since they were
never acknowledged, and a surprise if you thought shutting down was free. With
a handler that is not idempotent, that redelivery is a double charge. Pair with
[basic/05](../../basic/05-idempotent-consumer).

## Then

```bash
docker compose down
```

## Related

- [Consuming — concurrency](https://acemq-company.github.io/acemq-java-amqp/consuming.html#concurrency)
- [Consuming — prefetch](https://acemq-company.github.io/acemq-java-amqp/consuming.html#prefetch)
