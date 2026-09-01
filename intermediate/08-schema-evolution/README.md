# intermediate/08 — schema evolution

The question is never "can we add the field". It is "which of the twenty
services that read this message do we have to deploy, and in what order".

With a default on the new field, the answer is: none of them, in any order.

## What it demonstrates

- **Producer deployed first.** A consumer that has never heard of `currency`
  reads a message containing it, and the field is *skipped* rather than shifting
  every byte after it.
- **Consumer deployed first.** A consumer already on the new schema reads a
  message from a producer that is not, and the reader's default fills the gap —
  so the new code can be written as though the field were always there.
- **Both on the new version**, where the real value arrives rather than the
  default.
- **The same addition without a default is refused**, which is what makes the
  rule a rule.

## The one line that matters

```java
AvroCodec.registered(registry, V2)     // reader schema — mine
AvroCodec.registered(registry)         // no reader schema — whatever the writer sent
```

The second argument is what makes resolution happen. Without it a reader adopts
the **writer's** schema, so a field it has never heard of still arrives and a
field it expects is simply absent. That is not evolution; that is receiving
whatever the producer decided to send.

Requires **0.2.4 or later** — writing this example is what showed the overload
was missing, and it is the reason `0.2.4` exists.

## Always give a new field a default

```json
{"name": "currency", "type": "string", "default": "GBP"}
```

Without one there is nothing Avro can put there when an old producer omits the
field, so the read fails. In a deployment that means **every consumer breaking
the moment it is rolled out ahead of the producers** — which, since consumers are
usually deployed first, is most of the time.

The safe changes are: add a field with a default, remove a field that had one,
and rename via an alias. Everything else — changing a type, making an optional
field required — is a new message type wearing the old one's name.

## Running it

```bash
mvn compile exec:java      # no broker required
```

## What to expect

```
  old reader, new message   id=o-2 total=200 currency=(not in my schema)
  new reader, old message   id=o-1 total=100 currency=GBP
  new reader, new message   id=o-2 total=200 currency=EUR
  no default                refused, as it must
```

## Related

- [intermediate/07](../07-avro-and-the-registry) — where the schema identifier comes from
- [Serialization](https://acemq-company.github.io/acemq-java-amqp/serialization.html)
