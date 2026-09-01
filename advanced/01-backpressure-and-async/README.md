# advanced/01 — backpressure and async publishing

`send` waits for the broker to confirm each message before returning. That is the
right default — it is the only version where a returned call means the broker has
the message — and it costs a **network round trip per message**.

Publishing a thousand messages that way takes half a second, which looks like the
broker being slow and is really a thousand round trips being a thousand round
trips.

## What it demonstrates

| | 1,000 messages |
|---|---|
| `send` | 547 ms |
| `sendAll` | 26 ms |
| `sendAsync` | 17 ms |

All three end with every message confirmed. The alternative to `send` is **not**
giving up confirms — it is to stop waiting for them one at a time.

## The three shapes

```java
publisher.send(order);                 // out, confirmed, returned
publisher.sendAll(thousandOrders);     // all out, then all confirmed
publisher.sendAsync(order);            // a future you must eventually join
```

`sendAll` is what most bulk publishing actually wants: the throughput of
pipelining with the safety of having waited. It fails if **any** message failed,
and the exception says how many succeeded — a partial batch is the normal outcome
of a broker problem halfway through, and pretending otherwise leaves you
resending messages that already arrived.

It is **not atomic**. AMQP has no such thing: there is no way to publish a
hundred messages so that all or none arrive, and a library offering one would be
lying.

`sendAsync` is for the caller with something else to do meanwhile. A future
nobody joins is a message nobody knows the fate of.

## The bound is the point

```java
AceMq.connect(ConnectionConfig.url(url)
        .maxOutstandingPublishes(256)
        .build());
```

This caps how many messages may be in flight without a confirm. Once the window
is full the publisher **blocks** rather than queueing without limit in the client.

That bound is what stops a fast producer turning a slow broker into an
`OutOfMemoryError` on the *producer's* side — a failure that looks like a client
bug and is really an unbounded buffer. Pipelining without a ceiling is not faster
publishing; it is a memory leak with good throughput until it isn't.

Pair it with [intermediate/05](../../intermediate/05-blocked-connections):
`maxOutstandingPublishes` bounds the queue in your process, `blockedTimeout`
bounds how long a single publish waits when the broker stops accepting.

## Running it

```bash
docker compose up -d
mvn compile exec:java
```

## What to expect

```
  send        1000 messages in   547 ms
  sendAll     1000 messages in    26 ms
  sendAsync   1000 messages in    17 ms
  speedup     sendAll 21x, sendAsync 32x
  queued      3000 messages, all confirmed
```

Your numbers will differ; the shape will not. The test asserts only that
pipelining is several times faster and that a per-message confirm cannot be
instant — a version of this that ever ran in single-digit milliseconds would mean
confirms had quietly stopped being awaited.

## When to keep using `send`

When the message matters more than the throughput, and there is one of it. An
order taken from a user is a `send`. A nightly export of four million rows is a
`sendAll`.

## Then

```bash
docker compose down
```

## Related

- [Publishing](https://acemq-company.github.io/acemq-java-amqp/publishing.html)
