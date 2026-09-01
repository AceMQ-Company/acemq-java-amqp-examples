# advanced/05 — message expiry

A price quote, a session token, a "still typing" notice. Past a certain age,
handling one is worse than dropping it — and a consumer coming back after an
outage should not work through an hour of stale instructions before reaching the
current ones.

## What it demonstrates

- **`expiringAfter(Duration)`** puts a time to live on the message itself.
- **Expired messages are dead-lettered**, not deleted — if the queue has a
  dead-letter exchange. That is what makes "how many did we drop" answerable.
- **They expire at the head of the queue, not on a timer.** This is the part that
  catches people.

## Expiry is lazy

```
  after 3s   queued=0 expired=2
  behind a   long-lived message: queued=2 (q-3 has expired but is still counted)
  delivered  [blocker] — the expired one was dropped, not handled
  after the  blocker is consumed: queued=0 expired=3
```

RabbitMQ removes an expired message when it **reaches the head of the queue**, not
when its clock runs out. A short-lived message sitting behind a long-lived one
stays in the queue and stays *counted* until the message in front of it is dealt
with.

Consequences worth planning around:

- **Queue depth is not a count of live messages.** A monitor alerting on depth
  can be alerting on messages that are already dead.
- Expiry does not free memory or disk on schedule. A queue nobody reads holds its
  expired messages indefinitely.
- The message is dropped rather than delivered when the head reaches it, so a
  consumer never sees a stale one — the guarantee holds even though the
  bookkeeping lags.

## Give the queue somewhere to put them

```java
mq.declareQueue("quotes.new", QueueType.CLASSIC, Map.of(
        "x-dead-letter-exchange", "",
        "x-dead-letter-routing-key", "quotes.expired"));
```

Without this, expired messages simply vanish. With it, they land somewhere
countable — and "we dropped four thousand quotes last night" is the sort of thing
worth finding out from a graph rather than from a customer.

## Per-message or per-queue

| | Set by | Use when |
|---|---|---|
| Per message | `expiringAfter(...)` | The lifetime belongs to the message — a quote valid for 30 seconds |
| Per queue | `x-message-ttl` argument | The lifetime belongs to the destination — a notifications queue where nothing over a minute matters |

Both can apply, and the **shorter wins**. Per-queue is the one to reach for when
you want a guarantee that does not depend on every publisher remembering.

## Check the capability first

```
  supported  TTL_PER_MESSAGE=true
```

Per-message expiry is a capability, not something every transport has — the
in-memory transport does not claim it, which is why this example needs a broker.
See [advanced/04](../04-portability-and-capabilities).

## Running it

```bash
docker compose up -d
mvn compile exec:java
```

## Then

```bash
docker compose down
```

## Related

- [advanced/04](../04-portability-and-capabilities) — asking before using
- [basic/03](../../basic/03-retries-and-dead-letters) — the other route into a dead-letter queue
