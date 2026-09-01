# advanced/02 — encrypting payloads

TLS protects a message while it is moving. It does nothing about one sitting in a
queue that an operator, a backup, or anyone with the management UI can read.

The library's [security page](https://acemq-company.github.io/acemq-java-amqp/security.html)
says there is no payload encryption and tells you to do it in your own code.
**This is that code.**

## What it demonstrates

- **`Codec` is the seam.** It is the last thing to touch the bytes on the way out
  and the first on the way in, which is exactly where encryption belongs.
- **It wraps a delegate** rather than serialising anything itself, so the format
  and the decision to encrypt stay independent. JSON in, AES-GCM out.
- **A wrong key and a tampered message are the same answer.** GCM authenticates
  as well as encrypts; neither reaches the application.

```
  plain      {"id":"p-1","cardHolder":"A. Customer","amount":42.0}
  encrypted  <81 bytes of ciphertext>
```

The first line is what an operator sees today. The second is what they see after.

## Name the content type after the wire format

The first version of this used `application/vnd.acemq.encrypted+json`, because
the plaintext underneath is JSON. That was wrong, and the example printed the
proof:

```
  offered to json codec: true     ← before
  offered to json codec: false    ← after
```

A `+json` suffix is a **promise that the bytes on the wire are JSON**, and every
JSON-aware consumer reads it that way — this library's own codec volunteers for
any `application/*+json`. These bytes are ciphertext. With the wrong name, a
message ends up failing inside a parser rather than being refused by a codec that
knows it cannot help.

The content type describes the wire format, not what is under the encryption.

## A fresh nonce per message

```java
byte[] nonce = new byte[12];
random.nextBytes(nonce);
```

Reusing a nonce with GCM does not merely weaken the encryption, it **forfeits**
it: two messages under the same key and nonce leak their difference outright. The
nonce is prepended to the ciphertext, because the reader needs it and it is not a
secret.

## The exception says nothing about the payload

```java
throw new AceMqException("could not encrypt a " + payload.getClass().getName(), e);
```

Not the payload itself. An error message that helpfully prints what could not be
encrypted writes the plaintext into the log, which is the one place it was never
supposed to reach.

## This is an example, not a security product

What is missing, and what makes it real work:

- **The key comes from a `KeyGenerator` here.** It should come from a key
  management service.
- **No key identifier on the message**, so keys cannot be rotated without a flag
  day. A real version puts a key id in a header and keeps the previous key
  readable.
- **No answer for the operators.** Once the queue is opaque, the people who used
  to debug production by reading a message cannot. That is the actual cost of
  this pattern and it needs a decision, not a library.

Encrypt what genuinely needs it — card holders, health records, credentials — and
leave the rest readable. A system where every queue is opaque is a system nobody
can operate.

## Running it

```bash
mvn compile exec:java      # no broker required
```

## What to expect

```
  plain      {"id":"p-1","cardHolder":"A. Customer","amount":42.0}
  encrypted  <81 bytes of ciphertext>
  round trip [A. Customer 42.0]
  offered to json codec: false
  offered to this codec: true
  wrong key  refused
```

## Related

- [Security](https://acemq-company.github.io/acemq-java-amqp/security.html)
- [Serialization](https://acemq-company.github.io/acemq-java-amqp/serialization.html)
