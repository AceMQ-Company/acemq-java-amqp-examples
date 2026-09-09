# intermediate/12 — saga

Three services, no shared transaction, and what happens when the third one says
no. Three runs: one that works, one that is undone, and one that cannot be.

## What it demonstrates

- **`Saga.named(...).step(...).compensateWith(...)`** — a sequence where each
  step knows how to undo itself.
- **Compensations run backwards**, newest first.
- **A failed compensation is reported rather than thrown**, and the step it left
  behind is named.
- **Every step publishes**, so the broker's own record of each run is printed
  next to the result.

## What to look for

```
  run 1      complete=true steps=[reserve-stock, take-payment, book-courier]
             the broker saw: [stock.reserved, payment.taken, courier.booked]
```

The happy path, for contrast. Booking the courier has no compensation, and that
is a claim rather than an oversight: it is the last step, so nothing after it can
fail and there is nothing of it to undo. A step with no compensation belongs at
the end.

```
  run 2      complete=false compensated=true failedAt=book-courier
             because no courier will collect from that postcode
             the broker saw: [stock.reserved, payment.taken, payment.refunded, stock.released]
```

**Read the last line right to left.** The refund comes before the release,
because the later steps are the ones built on the earlier ones. Undoing them in
the order they were done would release the stock while the payment that paid for
it was still standing.

A saga returns this rather than throwing it. The caller has to decide what
happens next, and the interesting part is not the exception.

```
  run 3      unresolved=[reserve-stock] — the row a person has to look at
             the broker saw: [stock.reserved, payment.taken, payment.refunded]
```

**The third run is the one that matters.** A compensation itself fails — the
warehouse will not release a picked reservation — and the saga still does not
throw. The payment was refunded and the stock was not released, and nothing else
in the system knows that. No retry resolves it, because the warehouse's answer
will be the same next time.

`unresolved()` is the list to alert on. Everything else a saga reports is
recoverable by construction; these are real-world effects that happened, were
meant to be undone, and were not.

Note also that the refund still ran. A failed compensation does not stop the
ones after it: stopping leaves more undone than continuing does.

## What a saga is not

**Not a distributed transaction.** Nothing is isolated. After `take-payment` the
customer's money really has moved, and anybody looking sees that it has. If
`book-courier` then fails, the refund is a *new* fact rather than an erasure of
the old one, and for a few seconds the world contained a charge that should not
have happened. A customer whose card was charged and then refunded got two
emails from their bank.

That is not a defect. It is what compensating a real-world action means, and a
saga is honest about it where a two-phase commit pretends otherwise. It makes
the *end state* correct, not the middle, and choosing it means deciding that is
acceptable for this workflow.

So the steps have to be things that can be undone by doing something else.
Sending an email cannot be compensated — the apology is a second email, not an
unsend.

**Not durable.** This runs in one process and its state is on the stack. A crash
midway leaves the saga half-applied with nothing to resume it. Where a saga must
survive the process, the steps have to be messages and the state has to be in a
database; that is a much larger thing and it is not this class. For most systems
the in-process form is the right one, because it turns "remember to undo the
three things you already did" from a comment into something the compiler can see.

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

- [basic/06](../../basic/06-transactional-outbox) — the other half of the
  problem: making one step's effect and its event atomic
- [apps/01](../../apps/01-order-fulfilment) — the same three services, kept
  consistent by events instead
