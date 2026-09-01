# basic/04 — replay

The other half of dead-lettering. Capturing a failed message is easy; this is
getting it back once the cause is fixed.

## What it demonstrates

- **Look before you touch.** `replay.pending()` says how many are waiting.
  Replaying forty thousand messages into a consumer that is already behind is a
  decision, and that is the number the decision needs.
- **A bounded trial first.** `replay.replay(1)` moves one. In production you do
  this before you believe the fix, then `replayAll()` when you do.
- **Provenance survives.** The replayed message carries `replayedFrom()`,
  `replayCount()` and the original `error()`, and its `attempt()` is reset so it
  gets the whole retry ladder again rather than arriving exhausted.

## What replay does not do

It does not fix anything. Replaying into a consumer that still has the bug
refills the dead-letter queue, more slowly, with the replay count climbing —
which is why `replayCount()` is worth reading in a handler. A message on its
fifth trip is telling you something a reset attempt counter hides.

It is at-least-once: each message is published to the source queue and only then
acknowledged in the dead-letter queue, so a crash between the two replays it
again. Acknowledging first would lose it, which is the wrong way round for a tool
whose whole job is not losing things.

## Running it

```bash
docker compose up -d
mvn compile exec:java
```

## What to expect

```
  dead-lettered  3 orders while the service was down
  waiting        3 in orders.new.dlq
  replayed       1 as a trial, 1 handled
  replayed       2 more, 3 handled in total
  provenance     replayedFrom=orders.new.dlq replayCount=1 attempt=1
  why it failed  the inventory service is down
  dlq now        0
```

## Then

```bash
docker compose down
```

## Related

- [basic/03 — retries and dead letters](../03-retries-and-dead-letters), which puts them there
- [Reliability](https://acemq-company.github.io/acemq-java-amqp/reliability.html)
