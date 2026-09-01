# intermediate/02 — interceptors

Stamping a tenant on every message, timing every handler, counting failures.
Written by hand these are five lines repeated in forty places, and the bug is
always the place somebody forgot.

An interceptor is registered once on the connection and applies to everything
that goes through it.

## What it demonstrates

- **A publish interceptor adds a header to every message.** No publisher call
  site in this example mentions a tenant, and none of them can forget to.
- **`order()` decides what runs first**, not the order you happened to call
  `intercept(...)` in. The second interceptor reads the header the first one
  added, and that stays true when somebody reorders two lines of start-up code.
- **A consume interceptor sees the whole lifecycle**: `beforeHandle`,
  `afterHandle` with the `Ack` the library decided on, and `onError` with the
  handler's own exception.

## The context is replaced, not mutated

```java
return context.withEnvelope(context.envelope().toBuilder()
        .header("x-tenant", "acme")
        .build());
```

`beforePublish` returns a context rather than editing one. An interceptor that
mutated in place would be changing a message another interceptor had already
seen, and the resulting bug depends on ordering — the worst kind to reproduce.

## What each hook is for

| Hook | Fires | Use it for |
|---|---|---|
| `beforePublish` | Before the message leaves | Headers, tenancy, correlation, redaction |
| `afterConfirm` | Broker confirmed | Latency metrics, `routed()` — was anything listening |
| `beforeHandle` | Before your handler | Start a timer, set up a logging context |
| `afterHandle` | Handler returned | The `Ack`: accepted, retried, dead-lettered — the number worth graphing |
| `onError` | Handler threw | Error reporting, without wrapping every handler in try/catch |

`onError` runs **before** the failure policy, so it sees the original exception
rather than whatever the retry ladder decided to do about it.

## Running it

```bash
docker compose up -d
mvn compile exec:java
```

## What to expect

```
  handled    1, failed 1
  publish    [stamp:order.placed, audit:tenant=acme]
  consume    [received:tenant=acme, failed:negative total]
  confirms   [confirmed:routed=true]
```

`audit:tenant=acme` is the ordering working: the interceptor at `order()` 20 saw
what the one at 10 wrote.

## Keep them cheap

Every hook runs on the publish or handle path. An interceptor that makes a
network call has added that call to every message in the system — and one that
throws breaks messages it has nothing to do with. Record and move on; do the
slow part somewhere else.

## Then

```bash
docker compose down
```

## Related

- [Publishing](https://acemq-company.github.io/acemq-java-amqp/publishing.html)
- [Consuming — failure](https://acemq-company.github.io/acemq-java-amqp/consuming.html#failure)
