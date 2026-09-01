# basic/07 — serialization formats

JSON is the default and is the right answer most of the time. The reason to know
the others is migration: sooner or later a queue has to carry a format it was not
carrying yesterday, and how that goes depends on a property of the library rather
than on planning.

## What it demonstrates

- **A publisher writes one format**, chosen where the destination is named:
  `.asXml()`, `.asYaml()`, or nothing at all for JSON.
- **A consumer reads all of them.** It is never told what to expect. That
  asymmetry is the whole point — publishers migrate one at a time while a single
  unchanged consumer keeps working.
- **`asXml()` returns a new publisher** rather than mutating the one it was
  called on. A long-lived publisher that quietly changed what it writes would be
  worse than one that cannot.

## The dependency is the switch

`asXml()` needs `acemq-amqp-codec-xml` on the classpath, and `asYaml()` needs
`acemq-amqp-codec-yaml`. Both are `runtime` scope here: the code compiles without
them and fails at the call if they are missing. JSON needs nothing added.

```xml
<dependency>
  <groupId>org.acemq</groupId>
  <artifactId>acemq-amqp-codec-xml</artifactId>
  <version>0.2.3</version>
  <scope>runtime</scope>
</dependency>
```

## Choosing one

| | Use it when |
|---|---|
| JSON | The default. Assume this unless something below applies. |
| XML | Something at one end already speaks it — a SOAP service, a legacy partner, a schema you do not own. |
| YAML | A human reads the message. Configuration and pipeline definitions, not order events. |

None of these is faster in a way that will matter before other things do. Pick
the one the systems at either end already use.

## Running it

```bash
docker compose up -d
mvn compile exec:java
```

## What to expect

```
  one consumer read all three: [json=1.0, xml=2.0, yaml=3.0]
  formats        json=application/json xml=application/xml
```

Three formats went onto one queue and one consumer decoded all three.

## One format per queue, in real life

The example puts three formats on one queue to show that it works. Do not take
that as a recommendation. A queue whose messages might be any of three things is
one nobody can write a tool against — a dead-letter inspector, a replay script, a
schema check. Migrate a queue *through* mixed formats; do not live there.

## Then

```bash
docker compose down
```

## Related

- [Serialization](https://acemq-company.github.io/acemq-java-amqp/serialization.html)
