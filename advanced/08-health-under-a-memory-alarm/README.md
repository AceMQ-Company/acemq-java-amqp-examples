# advanced/08 — health under a memory alarm

A broker in a **real** memory alarm, a connection it has stopped reading, and
`/acemq-health` answering **200 UP** with the reason — in 19 microseconds.

## Why

An alarm is the one broker failure that looks like an application failure. The
connection is open, nothing has thrown, the socket is fine — and the broker has
quietly stopped reading it. Two things then go wrong at once if the health
check is naive.

It reports **DOWN**, and an orchestrator restarts the process into the same
blocked broker, having thrown away whatever it was holding. A fleet doing that
together stops draining the queues at the moment the broker most needs them
drained. Consumers are unaffected by an alarm; only publishers are paused, so
the instance that stays up is the instance that can help.

Or it **hangs**, which is worse, because a readiness endpoint that never answers
is a pod that never becomes ready. That is not hypothetical: the check is
usually a small round trip to the broker — declare something, count something —
and on a blocked connection that request is not refused. It goes unanswered, for
as long as the alarm lasts.

## What it demonstrates

- **`/acemq-health` answering `200 UP` while blocked**, with
  `"blocked": true` and `"blockedReason"` in the details.
- **The five facts the check reads, timed.** Nineteen microseconds, because
  every one of them is answered from state the broker already pushed to this
  connection.
- **A publish refused** with the broker's own reason, and
  `mayHaveBeenPublished=false`.
- **A control**, which is what makes the rest mean anything: an ordinary queue
  lookup started on that same connection that has still not returned five
  seconds later.
- **The watermark put back**, so the broker is usable afterwards.

## What it prints

```
  serving    [/acemq-metrics, /acemq-health, /acemq-info] on http://127.0.0.1:53474
  before     200 {"status":"UP","components":{"acemq":{"status":"UP","details":{"transport":"rabbitmq","open":true,"blocked":false,"inFlight":0}}}}

  alarm      dropping the memory high watermark to nothing, which is
             the state a production broker reaches under memory pressure
  blocked    the broker said so: low on memory
  facts      open=true transport=rabbitmq blocked=true inFlight=0 reason=low on memory
  facts      read in 19 us
  during     200 {"status":"UP","components":{"acemq":{"status":"UP","details":{"transport":"rabbitmq","open":true,"blocked":true,"inFlight":0,"blockedReason":"low on memory"}}}}
  publish    refused after 2 s: reason=low on memory mayHaveBeenPublished=false
  control    a queue lookup on this same connection has returned after 5s: false
  restored   the watermark is back at 0.4
  after      200 {"status":"UP","components":{"acemq":{"status":"UP","details":{"transport":"rabbitmq","open":true,"blocked":false,"inFlight":0}}}}
```

Nineteen microseconds against a queue lookup on the same connection that is
still waiting after five seconds. The difference is not that one is quicker. It
is that one does not ask.

## UP, and on 200

`status` stays `UP` and the response stays `200`, which is the whole point: a
readiness probe wired to this endpoint does not fail because the broker is
protecting itself. The reason is not thrown away either — it is in
`blockedReason`, in the broker's own words. `low on memory` is something an
operator can act on; `publish failed` is not.

That is also why it is not reported as degraded. Degraded is for this instance
being worse at its job than it should be. A block is the broker's state,
identical across every replica, so an alert that fires for all of them at once
is one no deployment can act on. The `.NET`, Go, Python and Ruby libraries and
the Spring Boot health indicator all answer the same way, and the sentence in
front of the reason is fixed because an alert rule matches on it.

## The alarm

```bash
rabbitmqctl set_vm_memory_high_watermark 0
```

A real alarm on a real node — the state a production broker reaches under memory
pressure, not a flag set on the client. The example runs it through whatever
command it is given, which is `docker exec acemq-alarm-broker rabbitmqctl` by
default and the test's own container id when it runs under Maven.

**It is put back**, to the image's default of `0.4`, in a `finally` that runs
however the example ends. Two reasons, and the second is the one that surprises
people:

- Left down, every publisher on that broker is refused until somebody notices.
- **Closing a blocked connection waits on a broker that is not reading**, so the
  restore has to happen before the connection closes, not after.

## A broker of its own

An alarm belongs to the node, not to the connection that provoked it. Every
publisher on that broker is refused for as long as it lasts, so pointing this
example at a shared broker means failing whatever else was publishing — and
failing it with "publishing is paused", which reads like a defect in whatever
reported it rather than like a neighbour holding an alarm.

The `docker-compose.yml` here starts a broker for this example alone, and the
integration test starts its own with Testcontainers and throws it away, which is
why running the whole repository's suite is safe.

## Running it

```bash
docker compose up -d
mvn compile exec:java
```

Point it elsewhere, and say how to reach `rabbitmqctl` on that broker's node —
everything after the URL is the command, taken as words so there is no shell
quoting to argue with:

```bash
mvn compile exec:java -Dexec.args="amqp://localhost:5672 docker exec my-broker rabbitmqctl"
```

## Then

```bash
docker compose down
```

## Related

- [intermediate/05](../../intermediate/05-blocked-connections) — what an alarm
  does to a *publisher*, on the in-memory transport, with no Docker. This one is
  about what an *orchestrator* is told, which only a real broker can answer.
- [intermediate/10](../../intermediate/10-graceful-shutdown) — the other place
  a connection that will not close costs you a grace period.
