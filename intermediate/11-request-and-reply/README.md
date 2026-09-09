# intermediate/11 — request and reply

Asking a question over the broker and waiting for the answer: one round trip,
three questions in flight at once, a caller written in another language, and a
request nobody answers.

## Read this before using it

Request/reply over a broker is synchronous calling in asynchronous clothes, and
it inherits the worst of both — the caller is blocked like an HTTP client, and
the failure modes are a message broker's. Where two services can speak HTTP or
gRPC, they should: those have timeouts, load balancing, circuit breakers and
tracing that a messaging library will not match.

What it is genuinely for is reaching a service that is *only* on the broker — no
HTTP endpoint, behind a firewall, or one worker among many where the broker is
already doing the load balancing. That case is real, and doing it by hand means
reply queues, correlation ids and a timeout somebody always forgets.

## What it demonstrates

- **`mq.requester()` and `mq.respond(...)`** — the handler is a plain function
  and never sees the reply queue or the correlation id.
- **One reply queue, several answers**, matched by correlation id rather than by
  arrival order.
- **The reply address is written twice**, and either one alone is enough.
- **A timeout is its own outcome**, and does not mean the work did not happen.

## The two addresses

```
  the request came from    reply-to property                                  acemq-reply-to header
  mq.requester()           acemq.reply.4ed050b8-562f-44c4-b43c-e4a69b620870   acemq.reply.4ed050b8-562f-44c4-b43c-e4a69b620870
  another language         <none>                                             pricing.replies.foreign
  an ordinary publish      <none>                                             <none>
```

A request names its reply queue in AMQP's own `reply-to` property **and** in the
`acemq-reply-to` header, always with the same value, and a responder reads the
header first and the property second.

That is not belt and braces. Go, Python and Ruby have only ever written the
header; Java and .NET only ever wrote the property. Writing both and reading
either is what makes a Java requester answerable by a Go responder and the other
way round — before it, a request crossed the language boundary, was decoded, was
handled, and the answer went nowhere.

The header is preferred for a second reason: it survives a hop that rebuilds the
message — a retry rung, a dead-letter, a shovel — and the AMQP property does not.

The middle row is a request published by hand with only the header on it, which
is exactly the shape the other four libraries send. It is answered.

## Correlation, not order

```
  asked      [SKU-1, SKU-2, SKU-3], and SKU-1 takes half a second to price
  answered   [SKU-2, SKU-3, SKU-1]
  matched    every answer reached the caller that asked for it
```

A requester owns one reply queue for its whole life, so every answer to every
caller arrives on the same queue. What sends each one to the right caller is the
correlation id and nothing else. SKU-1 is deliberately slow so the answers come
back in a different order from the questions — otherwise the run would prove
nothing that pairing them off by arrival would not also satisfy.

The three responders are given `ConsumerOptions.prefetch(1)`. At the default,
the first responder to ask is handed all three requests and prices them one
after another, which is the usual reason a queue with idle consumers still has a
backlog.

## A request nobody can answer

```
  unanswerable 1 — handled, counted, and not retried forever
```

A message that named no reply address at all is almost always a `publish` where
a `request` was meant. Failing the handler would retry it, then dead-letter it,
and produce a queue of messages whose only problem is that nobody asked for
anything. `Responder.unanswerable()` above zero means a caller is broken, and it
is a number worth graphing.

## A timeout is not a failure to do the work

```
  timed out  after PT0.75S
             a timeout does not mean the work did not happen: the request may be
             queued, running, or done with the reply lost
  counters   timedOut=1 unmatched=0
```

`RequestTimedOutException` has its own type because the answer is different from
every other failure here. Retrying is a decision about idempotency, not a
reflex — and where the work is not idempotent, a timeout is a question for a
person rather than for a retry loop.

`unmatched()` is the other half: replies that arrived with nobody waiting for
them. Under a slow responder it is the number that says the timeout is too short.

## The reply queue

`acemq.reply.<uuid>`, not durable, declared with `x-expires`. A reply queue
holds answers nobody will read once the asking process is gone, so outliving it
is pure cost — and a service that restarts often would otherwise leave thousands
of them behind, which is a real way to run a broker out of memory with nothing
obviously wrong.

One requester per connection. One per call would create and destroy a queue for
every question asked.

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

- [intermediate/01](../01-consumer-groups) — prefetch, and why it decides fairness
- [intermediate/10](../10-graceful-shutdown) — `close()` drains, which is what
  keeps a caller's request from becoming their timeout
