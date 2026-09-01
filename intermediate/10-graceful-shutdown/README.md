# intermediate/10 — graceful shutdown

Kubernetes sends SIGTERM and starts a clock. When it runs out the process is
killed, and whatever is still inside a handler dies with it. Those messages were
never acknowledged, so the broker redelivers them.

That is correct behaviour, and it is why a deployment shows up as a spike of
duplicate work when nobody arranged otherwise.

## The two calls are not the same

| | What it does |
|---|---|
| `close()` | Cancels the subscription and returns. **Does not wait** for a handler that is still running. |
| `drain(timeout)` | Cancels, then waits for the handlers to finish — and returns whether they did. |

The shape a service wants:

```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    if (!consumer.drain(Duration.ofSeconds(25))) {
        log.warn("shut down with work still in flight; it will be redelivered");
    }
    consumer.close();
    mq.close();
}));
```

`AceMq.drainConsumers(timeout)` does the same for every consumer on the
connection.

## The return value is the point

```
  enough time drained=true inFlight=0
  not enough  drained=false inFlight=1
```

`false` means the clock ran out with handlers still running. That is the signal
worth logging and alerting on, because it says one of two things:

- the grace period is shorter than the slowest handler, or
- a handler is stuck.

Both are worth knowing before they turn into a redelivery spike nobody can
explain. Set the drain timeout **slightly under** the grace period
(`terminationGracePeriodSeconds`), so draining loses the race to your own log
line rather than to SIGKILL.

## What redelivery costs you

Nothing, if the handler is idempotent. Everything, if it charges a card. This is
the same argument as [basic/05](../../basic/05-idempotent-consumer) and
[intermediate/09](../09-shared-idempotency) — a graceful shutdown reduces
duplicates, it does not eliminate them. A power cut has no SIGTERM.

## Running it

```bash
mvn compile exec:java      # no broker required
```

## What to expect

```
  enough time drained=true inFlight=0
  enough time handled=1 acknowledged=1, 9 left on the queue
  not enough  drained=false inFlight=1
  not enough  handled=1 acknowledged=1, 9 left on the queue
  no drain    closed without draining
  no drain    handled=1 acknowledged=1, 9 left on the queue
```

The nine left on the queue in every case are the ones never taken — prefetch
brought a few over, the handler works through them one at a time, and the rest
were always going to be somebody else's.

**The handled counts are the same in all three**, and that is an artefact of the
example rather than a result: this is one JVM that keeps running, so the abandoned
handler finishes anyway. In a real pod that thread is killed. What the example can
show honestly is `drained` and `inFlight` — the numbers you would act on.

## Related

- [Consuming](https://acemq-company.github.io/acemq-java-amqp/consuming.html)
- [intermediate/01](../01-consumer-groups) — draining a whole group
