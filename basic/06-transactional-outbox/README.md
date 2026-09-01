# basic/06 — transactional outbox

Saving an order and publishing an event are two writes to two systems. Done in
sequence, a crash in between leaves one done and the other not — an order nobody
was told about, or a notification for an order that does not exist. Retrying does
not help: the process that would retry is the one that died.

## What it demonstrates

- **One write, not two.** The message is inserted in the *same database
  transaction* as the order, using the connection the application is already
  holding. It becomes durable exactly when the order does.
- **A rollback takes the message with it.** The example places a second order and
  rolls it back. Neither the row nor the event survives — there is no window in
  which one exists without the other.
- **The relay publishes afterwards**, separately, often in another process.

```mermaid
flowchart LR
    A["placeOrder()"] --> T{{"one transaction"}}
    T --> O[("orders row")]
    T --> B[("acemq_outbox row")]
    T -.->|rollback| X["neither survives"]
    B --> R["OutboxRelay<br/>polls and publishes"]
    R --> Q[["orders.new"]]
```

## Why the consumer reads text

An outbox stores a payload that is **already serialised** — that is what made it
safe to write inside the transaction — and the relay republishes those bytes
unchanged. The consumer therefore receives exactly the string that was stored and
parses it with whatever the application already uses.

That is worth knowing before you design around it: `mq.consume(queue,
OrderPlaced.class, ...)` will not decode an outbox message, because the relay
publishes the stored text rather than re-serialising an object it never had.

## Running it

```bash
docker compose up -d
mvn compile exec:java
```

H2 runs in memory inside the process; only RabbitMQ needs the container.

## What to expect

```
  committed  order o-1 and its event, in one transaction
  rolled back order o-2 — neither the row nor the event survived
  in the db  orders=1, outbox rows pending=1
  published  [o-1 {"id":"o-1","total":42.0}]
  outbox now pending=0
```

## The other half

The outbox gives you at-least-once: the relay may publish a message twice if it
dies between publishing and marking the row done. Pair it with
[basic/05](../05-idempotent-consumer) — the outbox makes sure the message is
*sent*, and the idempotent consumer makes sure it is *handled once*.

## Then

```bash
docker compose down
```

## Related

- [Reliability](https://acemq-company.github.io/acemq-java-amqp/reliability.html#the-transactional-outbox)
