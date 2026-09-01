# basic/05 — idempotent consumer

Every broker worth using delivers at least once. Duplicates are normal traffic,
not an error, and handling one twice is your problem to prevent.

## What it demonstrates

The same run, twice over:

- **Without a store** — the same message arrives twice and the card is charged
  twice. This half matters: without it the example would be suppressing a
  duplicate that was never delivered, and showing you a guarantee it had not
  exercised.
- **With a store** — handled once, and `consumer.duplicates()` counts the one it
  suppressed.

## How it works

Claim, then confirm on success or release on failure. Marking a message handled
*before* the handler runs loses the work if the process dies mid-handler; marking
it *after* lets two concurrent deliveries both pass the check. The three-step
shape closes both.

```java
ConsumerOptions.prefetch(1).idempotent(InMemoryIdempotencyStore.forOneDay())
```

## The limit of this example

`InMemoryIdempotencyStore` deduplicates **within one process** and forgets
everything on restart. That is enough when duplicates arrive seconds apart on the
same instance, and useless the moment three instances sit behind one queue: the
redelivery lands on a different machine, finds an empty map, and charges the card
again.

For that, `JdbcIdempotencyStore` puts the claim in a table every instance can
see — and brings a failure the in-process one cannot have, because a consumer
that dies mid-handler leaves its claim behind. That is why a claim there is a
*lease* rather than a lock.

## Running it

```bash
docker compose up -d
mvn compile exec:java
```

## What to expect

```
  no store       card charged 2 times for one order
  with a store   card charged 1 time, 1 duplicate suppressed
```

## Then

```bash
docker compose down
```

## Related

- [Reliability](https://acemq-company.github.io/acemq-java-amqp/reliability.html#idempotency)
