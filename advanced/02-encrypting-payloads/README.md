# advanced/02 — encrypting payloads

TLS protects a message while it is moving. It does nothing about one sitting in a
queue that an operator, a backup, or anyone with the management UI can read.

The library ships this: `acemq-amqp-crypto`. Earlier versions of this example
hand-rolled the codec because there was none, and the list of things it said a
real version would need — a key identifier per message, rotation without a flag
day — is now the interesting part of the example rather than its caveat.

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

Reusing a nonce with GCM does not merely weaken the encryption, it **forfeits**
it: two messages under the same key and nonce leak their difference outright. The
nonce is prepended to the ciphertext, because the reader needs it and it is not a
secret.

## The exception says nothing about the payload

The codec's failure messages name the payload's **type** and never its value. An
error that helpfully prints what could not be encrypted writes the plaintext into
the log, which is the one place it was never supposed to reach.

## Rotation is the point

```java
Keyring keys = Keyring.builder()
        .add("payments-2026-06", june)      // still on some queue somewhere
        .current("payments-2026-09", now)   // everything written from here
        .build();
```

**The key identifier travels in the message, in the clear**, in front of the
ciphertext. A consumer reads which key a message needs rather than assuming the
current one, so a new key can be introduced while messages written with the old
one are still queued.

An AMQP header would have been tidier and would have lost it: headers are dropped
by shovels, rewritten by federation, and absent from a message recovered out of a
backup, and a ciphertext whose key nobody can name is gone.

## What still needs a decision from you

- **The key comes from `Keys.generate()` here.** In production `Keyring` is a
  small class in front of a key management service.
- **No answer for the operators.** Once the queue is opaque, the people who used
  to debug production by reading a message cannot.
  `EncryptedCodec.keyIdOf(body)` tells them which key a message needs without
  holding any — usually the whole question, since an undecryptable queue is
  normally a key retired too early. It is not a substitute for deciding how
  support reads a message before you turn this on.
- **Encryption is not authorisation.** Every service holding the keyring reads
  every message under those keys. Separate audiences mean separate keys.

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
  encrypted  <100 bytes of ciphertext>
  round trip [A. Customer 42.0]
  offered to json codec: false
  offered to this codec: true
  june message needs key payments-2026-06, and still reads: p-1
  new messages are written with payments-2026-09
  retired key refused: this message was encrypted with key 'payments-2026-06', which is not in the keyring.
```

## Related

- [Security](https://acemq-company.github.io/acemq-java-amqp/security.html)
- [Serialization](https://acemq-company.github.io/acemq-java-amqp/serialization.html)
