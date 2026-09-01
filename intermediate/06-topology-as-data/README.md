# intermediate/06 — topology as data

Topology declared by scattered `declareQueue` calls is topology nobody can read.
What exists depends on which services started, in what order, with which version
deployed.

Declared as a value it can be printed, diffed, reviewed in a pull request, and —
the part that matters during a deployment — **planned against what is really
there before anything changes**.

## What it demonstrates

- **One readable declaration** of what a service needs.
- **`plan()` changes nothing** and answers the deployment's question: what would
  this do?
- **`DRY_RUN` goes through the same path** and still changes nothing, which is
  what makes it worth putting in a pipeline as a check rather than a comment.
- **`CREATE_ONLY` applies what is missing** and is safe to run on every start-up.

```java
Topology.define()
        .exchange("orders", "topic")
        .classicQueue("orders.new", Map.of())
        .bind("orders.new", "orders", "order.placed")
        .build();
```

## Why a re-plan is not empty

Run it twice and the plan still reports three of five actions as creations:

```
  again      3 of 5 still reported as changes
    topology plan:
      create   exchange orders (topic)
      present  queue orders.new (classic)
      present  queue orders.audit (classic)
      create   binding orders.new <- orders [order.placed]
      create   binding orders.audit <- orders [order.*]
```

Queues are inspected, so they become `present`. **Exchanges and bindings cannot
be.** AMQP offers no way to ask whether one exists except a passive declare,
which kills the channel when the answer is no. Since redeclaring an equivalent
exchange or binding is harmless, the planner reports them as creations, because
that is honestly what apply will attempt.

Worth knowing before you build a "no changes expected" gate around
`hasChanges()`: it will never be false. The reliable signal is the **queue**
lines.

## CREATE_ONLY is not timid, it is correct

There is no mode that edits a queue in place, because AMQP has none. Most queue
arguments are immutable after declaration, so a change that looks like an edit is
really a delete and a recreate — which discards the messages in it. That is a
migration, and a migration should be a decision rather than a side effect of a
start-up.

`VALIDATE` exists for the deployment that wants to assert the topology is already
correct and fail otherwise.

## Running it

```bash
mvn compile exec:java      # no broker required
```

## What to expect

```
  plan       hasChanges=true
    topology plan:
      create   exchange orders (topic)
      create   queue orders.new (classic)
      create   queue orders.audit (classic)
      create   binding orders.new <- orders [order.placed]
      create   binding orders.audit <- orders [order.*]
  dry run    5 actions, still nothing created
  applied    5 created
  again      3 of 5 still reported as changes
  routing    orders.new=1 orders.audit=1
  next       the only new queue is:
    CREATE queue orders.fraud (classic)
```

## Related

- [Getting started](https://acemq-company.github.io/acemq-java-amqp/getting-started.html)
