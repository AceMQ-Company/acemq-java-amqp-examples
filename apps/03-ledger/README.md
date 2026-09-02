# apps/03 — event-sourced ledger

The log **is** the system of record. Balances are not stored; they are what you
get by adding up the log, and can be deleted and rebuilt at any time.

[apps/01](../01-order-fulfilment) and [apps/02](../02-policy-administration)
publish events describing what happened to a system of record that lives in a
database. Here there is no such database. Every entry is appended to a stream and
nothing is ever updated or deleted — money moved wrongly is corrected by posting
the opposite entry, exactly as a paper ledger does, and both entries stay.

That is what lets a ledger answer *"what did we believe on Tuesday"*, which is
the question auditors actually ask and the one a mutable balances table cannot
answer at all.

## Why a stream and not a queue

**A queue is emptied by being read. A stream is not.** That single difference is
the reason this application uses one:

- the writer reads the whole journal at start-up to recompute balances;
- a statement projection reads the same journal, from the same offset, at the
  same time, and neither reader affects the other;
- a projection written next year starts at offset zero and gets all of history.

On a queue, exactly one of those readers would get each entry. Which is correct
for a *command* — a transfer must be applied once — and wrong for a *fact*.

Note the two together in `Ledger.topology()`: commands go to an ordinary queue,
entries go to a stream. Getting that backwards is the most common mistake in
event-sourced systems.

## The modules

| Module | |
|---|---|
| **ledger** | The only writer. Decides whether a transfer is allowed and appends the entries |
| **projections** | A statement per account, built by reading from offset zero. Stores nothing the log does not contain |
| **transfers** | Where transfers are asked for, and refusals noticed |

## One writer, deliberately

Every transfer produces two entries that sum to zero. That invariant cannot be
enforced by two processes appending independently — a stream will happily accept
an unbalanced pair from each of them. Making the writer singular is what makes
the invariant checkable at all.

It is also what makes the writer's own balance tracking correct: after rebuilding
from the journal it maintains its own totals, which is safe *because* nothing else
writes.

## The bug this design has already had

The first version of `Balances` kept following the stream **and** applied each
entry as the writer wrote it. Every entry was therefore counted twice — once
locally, once when it came back round — and an account ended up with double its
balance.

The fix is the shape the code now has: **read to the end, then stop reading.**
Keeping only the stream has the opposite problem, a transfer decided against a
balance that does not yet include the transfer before it.

It is worth knowing that this is the failure mode. A projection that both
subscribes and self-updates is a natural thing to write and silently wrong.

## Amounts are integers

```java
public record EntryPosted(..., long amountMinor, ...) { }
```

Whole minor units — pennies, cents. A ledger in `double` is a ledger that
disagrees with itself after enough additions, and the disagreement appears in
production, at scale, in the direction nobody expected.

Signed, too, rather than a debit/credit flag: a sum over a column is then simply
a sum, and "which sign means debit" stops being a question every reader answers
for themselves.

## Running it

```bash
docker compose -f ../01-order-fulfilment/docker-compose.yml up -d   # just the broker
mvn -pl apps/03-ledger/system-test -am verify
```

RabbitMQ streams need no plugin — `x-queue-type: stream` is core since 3.9 and
reachable over AMQP 0-9-1, which is why this runs against the same container as
the other two applications.

## What is honestly not here

- **Snapshots.** A rebuild is O(history), and at a billion entries that stops
  being free. The answer is "the balance at offset N, plus everything after N".
  A real technique, deliberately omitted: it is the second thing to build, and
  including it would make event sourcing look cheaper than it is.
- **Atomic double entry.** The two halves of a transfer are appended one after
  the other. A real ledger appends them as a single record precisely so that a
  crash between them is impossible; here a crash between the two lines would
  leave the journal unbalanced.
- **Retention.** The journal keeps an hour, because this is a test. A real one
  keeps them for as long as the law says. **If retention is shorter than
  "forever", the projection is the system of record after all** — and nobody
  wrote that down.

## Related

- [Streams](https://acemq-company.github.io/acemq-java-amqp/streams.html)
- [basic/09-streams](../../basic/09-streams) — offsets and replay, one idea at a time
