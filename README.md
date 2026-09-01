# AceMQ for Java — examples

Runnable examples for [AceMQ for Java](https://github.com/AceMQ-Company/acemq-java-amqp).
Each one is a self-contained Maven module: open a directory and the whole example
is in front of you, with no shared helpers to trace.

They depend on the **released** version from
<https://acemq-company.github.io/maven/>, so they resolve exactly what the
documentation tells you to depend on.

## Running one

```bash
cd basic/01-publish-and-confirm
docker compose up -d
mvn compile exec:java
```

The TLS example needs certificates first, which is one command:

```bash
cd basic/02-tls-and-credentials
mvn -Pgencert          # writes ./certs and the rabbitmq.conf compose mounts
docker compose up -d
mvn compile exec:java
```

Java 17 or later, and Docker.

## What is here

### basic

| | |
|---|---|
| [01 — publish and confirm](basic/01-publish-and-confirm) | The smallest useful thing: publish, have the broker confirm it, consume it. Also shows an unroutable message being refused rather than silently dropped. |
| [02 — TLS and credentials](basic/02-tls-and-credentials) | Connecting over `amqps://` with generated development certificates, credentials that can rotate, and the two refusals the security policy makes on your behalf. |
| [03 — retries and dead letters](basic/03-retries-and-dead-letters) | A failure that recovers, one that never does, and one that was never going to. Retries scheduled by the broker rather than by a sleeping thread. |
| [04 — replay](basic/04-replay) | Getting dead-lettered messages back once the cause is fixed: a bounded trial first, then the rest, with provenance on every one. |
| [05 — idempotent consumer](basic/05-idempotent-consumer) | The same message delivered twice, charged once. Shown against the same run without a store, so the duplicate is real. |
| [06 — transactional outbox](basic/06-transactional-outbox) | The dual-write problem: an order and its event in one transaction, and a rollback that takes the event with it. |
| [07 — serialization formats](basic/07-serialization) | JSON, XML and YAML on one queue, read by a consumer that was never told which to expect. What makes a format migration two ordinary releases. |
| [08 — ordered per key](basic/08-ordered-per-key) | Order within an account, parallelism across accounts. Partitions, and the concurrency limit they impose. |
| [09 — streams](basic/09-streams) | A log that is read rather than emptied: checkpointing, resuming from an offset, and a second reader that still sees everything. |
| [10 — pipelines](basic/10-pipelines) | Three steps with a queue between each, retries and concurrency set per step, and a filtering step that ends the route without failing. |

### intermediate

| | |
|---|---|
| [01 — consumer groups](intermediate/01-consumer-groups) | Scaling a running group from one consumer to eight, what prefetch does to fairness, and draining before shutdown so nothing in flight is redelivered. |
| [02 — interceptors](intermediate/02-interceptors) | Cross-cutting concerns registered once on the connection: a tenant header no call site mentions, and the failure hook that replaces try/catch in every handler. |
| [03 — telemetry and tracing](intermediate/03-telemetry) | One trace across two broker hops, with a 70-line tracer you can read. Four operations, one trace id, exactly one root. |
| [04 — testing without a broker](intermediate/04-testing-without-a-broker) | `memory://` and a unit test that runs in 0.29s. What the in-memory transport refuses to fake, and why that is the point. |
| [05 — blocked connections](intermediate/05-blocked-connections) | The broker runs out of disk and stops accepting publishes. What the exception tells you, why an idle connection never finds out, and the 30-second default that decides whether an alarm degrades a service or stops it. |
| [06 — topology as data](intermediate/06-topology-as-data) | Exchanges, queues and bindings as one readable value, planned before applied — and why a second plan is never empty. |
| [07 — Avro and the schema registry](intermediate/07-avro-and-the-registry) | A schema identifier on the front of every message, 17 bytes on the wire, and the two framings that must never be mixed. |

More to come: 
connections, topology as data, schema evolution, and a full application under
`apps/`.

## Every example is tested

Each has an integration test that runs the actual `main` method against a real
RabbitMQ in a container and checks what it printed. That is not ceremony: an
examples repository rots quietly, and the first thing a newcomer meets is then a
build error. Here it is a red build instead.

```bash
mvn verify        # runs every example against a throwaway broker
```

## Contributing an example

Keep it to one idea, make the output say what happened, and add the test. An
example that cannot be tested is usually an example doing too much.

Each example carries a `README.md` saying what it is, what it demonstrates, how
to run it and what to expect. Add a diagram only where the shape of something is
the hard part — routing, or a trust chain. A diagram of two boxes and an arrow
costs more to maintain than it explains.
