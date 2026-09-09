# intermediate/13 — delivering a message later

Three reminders: one already overdue, one in three seconds, one in five. No
scheduler process, no plugin, no cron.

## What it demonstrates

- **`Scheduler.on(mq)`, `in(...)` and `at(...)`** — a delay without a timer in
  your process.
- **A ladder of queues, each with a uniform time to live**, hopped through until
  the message is due.
- **The accuracy that buys, and what it costs.**
- **The payload is encoded once** and carried as bytes, with its content type in
  a header of its own.

## What to look for

```
  scheduled 3, delivered 3, hops 6

  asked for   arrived at
       past      0.0s   R-0
         3s      2.0s   R-1
         5s      4.1s   R-2

  content type on arrival: application/json
```

**Read the two columns together. A three-second delay lands at about two.** A
message ships as soon as less than a second is left, because another hop through
the smallest rung would cost more than the accuracy it buys.

That is the trade this design makes, and it is stated rather than hidden:
delivery is accurate to about the smallest rung. Something that must fire at
09:00:00.000 wants a scheduler, not a message broker.

**`hops 6`** is the other number. R-0 was due already and took none; the other
two took three each. A one-day delay costs twenty-four hops and a one-minute
delay costs one — long delays are several broker round trips rather than one,
which is the honest cost of not requiring a plugin.

A message whose time has already passed is delivered at once rather than
refused. A renewal date that has gone by is a reminder that is late, not an
error.

## Why not a per-message time to live

Because a classic queue expires messages only from its **head**. Put a four-hour
message in and a one-minute message behind it, and the one-minute message is
delivered in four hours — and nothing reports it. The queue looks healthy, the
message is not lost, it is simply late by a factor nobody predicted.

It is the single most common way a home-made scheduler fails, and it fails in
production under mixed load rather than in testing under uniform load.

The ladder avoids it by giving every message in a rung the same delay, so the
head is always the message due soonest:

```
acemq.schedule.1h  acemq.schedule.10m  acemq.schedule.1m  acemq.schedule.10s  acemq.schedule.1s
```

Each expiry dead-letters the message back into `acemq.schedule.due`, where the
scheduler either delivers it or puts it in the largest rung that does not
overshoot. A ninety-second delay is one minute, then three tens.

The alternative is RabbitMQ's delayed-message-exchange plugin, which does this
properly and is a plugin — so it is not available everywhere, and a library that
silently required it would be a library that works on your laptop.

## The names are the contract

`acemq.schedule.{1h,10m,1m,10s,1s}`, `acemq.schedule.due` and the headers a
scheduled message carries are shared with the Go, Python, Ruby and .NET
libraries. Two services scheduling on one broker declare the same queues, and a
rung declared with a different time to live is a `PRECONDITION_FAILED` for
whichever declares second. Nothing about it is a local decision.

The queues are an implementation detail in every other respect: publishing into
them by hand produces a message the scheduler will refuse, because it arrives
without the headers a scheduled message carries.

## The content type travels separately

The scheduler encodes the payload once, when it is scheduled, and moves bytes
from then on — it never looks inside a message it has no business understanding.
The content type goes in a header and is put back on the message finally
delivered. Without that, what arrives is the right bytes under
`application/octet-stream`, and the consumer that was waiting for it cannot read
them.

## Running it

```bash
docker compose up -d
mvn compile exec:java
```

It takes about five seconds, because it is waiting for real delays.

## Then

```bash
docker compose down
```

## Related

- [advanced/05](../../advanced/05-message-expiry) — the per-message time to
  live, and the head-of-queue behaviour this design works around
- [basic/03](../../basic/03-retries-and-dead-letters) — the same ladder idea,
  used for retries
