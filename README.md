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

More to come: retries and dead letters, idempotency, the transactional outbox,
streams, pipelines, and a full application under `apps/`.

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
