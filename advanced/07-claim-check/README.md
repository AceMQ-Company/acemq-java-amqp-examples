# advanced/07 — claim check

Two documents that differ by **one byte**, either side of the threshold. One
travels on the broker; the other goes to a store and the message carries a key.

## Why

A scanned report is tens of megabytes. Putting it on a queue is possible and is a
mistake: it sits in the broker's memory, it is copied to every bound queue, it
makes a dead-letter queue impossible to look at by hand, and it turns a broker
into a filesystem with worse tools. Nothing refuses a 40 MB message — that is the
problem. It simply makes everything worse afterwards.

The other half is the part that is easy to get wrong. Offloading a two-hundred-
byte message turns one broker round trip into a store round trip *and* a broker
round trip, so an unconditional claim check makes the common case slower in order
to fix the rare one.

## What it demonstrates

- **`ClaimCheckCodec.wrapping(delegate, store)`** — the delegate serialises, this
  decides where the bytes end up.
- **The threshold, from both sides, in one run.**
- **Three bytes of framing** that let one consumer read either shape.
- **A message written without this codec still reads**, which is what makes
  introducing it a release rather than a flag day.
- **What happens when the store no longer has the payload.**

## The boundary

```
  document         serialised  on the wire   framing    claim check
  doc-just-under        65535        65538   AC 01 00   -
  doc-exactly           65536           39   AC 01 01   dec117b6-c174-4b5a-a090-bacb56a46e83

  in the store  dec117b6-c174-4b5a-a090-bacb56a46e83 is 65536 bytes, and the broker never saw them
```

One byte more in the document, and 65 kilobytes less on the broker.

The threshold is 64 KiB and the comparison is **strictly less than**: a payload
*smaller* than the threshold travels inline, so a payload of exactly 65536 bytes
is the one that is offloaded. That is worth stating precisely, because an
off-by-one here is invisible — messages still arrive, still decode, and the only
symptom is a broker holding payloads it was supposed to be spared.

It is measured against what the delegate produced, not against the object, which
is why the two documents in this example are built by measuring rather than by
guessing.

## What is on the wire

```
0xAC  0x01  0x00  payload      inline, and identical to what the delegate wrote
0xAC  0x01  0x01  key          a claim check
```

Three bytes, and they are why the threshold can be changed — or this codec
introduced — without a flag day. A consumer handles both shapes without being
told which it is about to get, and messages written before the change are still
readable after it.

The content type stays the delegate's, unchanged. Unlike encryption, where the
bytes really are something else, a claim-checked document is still a document —
it is a document that is somewhere else. A consumer that lacks the store gets a
clear failure rather than a parser error.

`ClaimCheckCodec.keyOf(body)` reads the key without fetching it, which is the
question an operator looking at a dead-letter queue actually has: which object
does this need, and is it still there.

## An unframed message

```
  unframed      a message with no framing still reads: true
```

Written before this codec existed, or by a publisher that does not use it.
Reading it as the delegate would is the only useful answer, and it is what makes
adding a claim check to a live queue an ordinary release.

## Retention outlasts everything

```
  deleted       the claim check 'dec117b6-…' is not in the store, so this message cannot be read.
```

The payload was removed while a message referring to it was still deliverable.
The store's retention has to outlast **every queue the message can reach, every
dead-letter queue behind them, and any replay somebody does by hand** — which is
a longer life than the obvious "delete it once it has been consumed" suggests.

## Choosing a store

This example uses `FilesystemClaimCheckStore`, which is the honest middle ground:
useful where the filesystem is shared and durable — an NFS mount, a persistent
volume — and, on a container's local disk, the in-memory store with extra steps,
because the consumer is on another host and finds nothing.

Object storage is the usual right answer, and a `ClaimCheckStore` in front of S3
or Azure Blob Storage is three short methods.

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

- [advanced/02](../02-encrypting-payloads) — the other codec that wraps a
  delegate, and why its content type does change
- [basic/07](../../basic/07-serialization) — what the delegate is doing
