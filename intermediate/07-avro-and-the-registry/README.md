# intermediate/07 — Avro and the schema registry

Avro bytes describe nothing about themselves. Given a payload and no schema there
is nothing you can do with it — that is the trade for how small it is.

A registry closes the gap: the writer puts a schema identifier on the front of
every message, and a reader looks the identifier up to find the schema that wrote
it.

## What it demonstrates

- **`AvroCodec.registered(registry)`** writes a five-byte header — a zero byte,
  then four bytes of identifier, big-endian — before the Avro body. That is the
  layout Confluent's clients use, so messages written here are readable by them
  and the other way round.
- **`definitionOf(schema)`** turns a schema into what a registry stores, so it can
  be registered before anything is published.
- **`fingerprint()`** answers "is this the same schema?" without comparing two
  blobs of JSON. It is how a registry avoids issuing a second identifier for a
  schema it already holds.
- **Seventeen bytes** for a record with a three-character id and a double.

## Why the size is the point

Five bytes of framing plus twelve of body. The field names live in the schema,
not in every copy of the message — the same record as JSON is roughly twice that
and repeats `"id"` and `"total"` a billion times a day at scale.

## Two framings, never mixed

| Codec | Content type | Schema comes from |
|---|---|---|
| `AvroCodec.of(schema)` | `avro/binary` | Compiled into both ends |
| `AvroCodec.registered(registry)` | `application/vnd.acemq.avro` | The identifier in each message |

They are **not interchangeable**, and the difference is invisible in the bytes: a
fixed-schema codec reading a registered message would take the five framing bytes
as the start of the first field.

Writing this example is how that was found. On `0.2.3` the mismatch decodes into
silent nonsense — an empty id and a total of `5.4e-67`, no exception. The library
now refuses both directions; that fix is in the release after `0.2.3`, which is
why this example demonstrates the content types rather than the failure.

## Fixed schema or registry?

Use the **registry** unless there is a reason not to. `of(schema)` means the
writer's schema is whatever the reader happens to have compiled in, so the moment
a producer adds a field every consumer still holding the old schema reads the new
bytes wrongly. Sound only where producer and consumer are released together.

The registry in this example is a `HashMap`. In production it is Confluent's,
Apicurio, or a table in your own database — `SchemaRegistry` is two methods.
**Note that only the in-memory implementation ships today**; persisting it is
yours to provide.

## Running it

```bash
mvn compile exec:java      # no broker required
```

## What to expect

```
  registered id=1 subject=org.acemq.examples.OrderPlaced
  fingerprint 1a39a1ec78ccebe151264c1c1c7924ee66e562dca24e67ef289dcaefc6e9158d
  received   [o-1=42.0]
  on the wire 17 bytes, id=1, content-type=application/vnd.acemq.avro
  framings   registered=application/vnd.acemq.avro fixed=avro/binary, never mix them
```

## What a GenericRecord cannot do

Avro's reader-schema resolution — dropping a field the reader does not know,
filling in a default for one the writer omitted — needs a **reader schema**. A
`GenericRecord` asks for nothing in particular, so the writer's schema is used
for both and no resolution happens. Real evolution needs a generated
`SpecificRecord` whose class carries the reader schema.

## Related

- [Serialization](https://acemq-company.github.io/acemq-java-amqp/serialization.html)
