# advanced/06 — multi-tenant topology

The cheap answer is a tenant field in the payload and one shared queue. It works
until one customer sends ten million messages, or a poison message from one
tenant stops the consumer serving all of them, or somebody has to prove to an
auditor that tenant A's data was never readable by tenant B.

A queue per tenant costs a little topology and answers all three.

## What it demonstrates

- **The broker does the separation.** The routing key carries the tenant, so a
  consumer never filters and cannot forget to.
- **The topology is generated from the tenant list**, not written out once per
  customer. Adding a customer is a list entry.
- **Blast radius**: one tenant floods, the others are untouched.

```
  acme saw   [a-1]
  after a flood by globex:
  acme       queue holds 0
  globex     queue holds 501
  initech    queue holds 1
```

## Tenant in the name, not in the payload

```java
private static String queueFor(String tenant) {
    return "orders." + tenant;
}
```

This looks like a style choice and is not. **A permission in RabbitMQ is a
regular expression over names.** `orders\.acme\..*` is grantable; "the messages
whose tenant field says acme" is not expressible as a permission at all.

Naming is what makes per-tenant credentials possible. Without it, every service
holds credentials that can read every tenant's queue, and the isolation exists
only in code that could have a bug.

## What a queue per tenant buys you operationally

It is the unit you can act on:

| | With one shared queue | With a queue per tenant |
|---|---|---|
| Depth alerting | One number for everyone | Per customer — you can see *whose* backlog |
| A poison message | Blocks every tenant | Blocks one |
| Rate limiting | Not possible | Per queue |
| Pause a customer | Not possible | Stop that consumer |
| Dead letters | Mixed together | One per tenant, separately triageable |
| Proving isolation | An argument about code | A permission |

## Generate it, do not repeat it

```java
for (String tenant : TENANTS) {
    builder.classicQueue(queueFor(tenant), Map.of())
           .bind(queueFor(tenant), "orders", "order." + tenant + ".*");
}
```

Every tenant is configured identically because the same loop configured them all.
Copy-pasted topology is where "why does tenant C have no dead-letter queue" comes
from — and that question is always asked during an incident involving tenant C.

Pair with [intermediate/06](../../intermediate/06-topology-as-data): the plan
tells you what adding a customer will actually create before it creates it.

## When not to do this

Thousands of tenants is thousands of queues, and each one costs memory and a
little management overhead. Past a few hundred, the usual shapes are a queue per
*tier* with the largest customers separated out, or a vhost per tenant when the
isolation needs to be absolute.

The tenant-in-the-payload version is not wrong for a handful of internal
consumers with no isolation requirement. It is wrong the moment "customer" means
"someone with a contract".

## Running it

```bash
mvn compile exec:java      # no broker required
```

## Related

- [intermediate/06](../../intermediate/06-topology-as-data) — planning topology before applying it
- [Security](https://acemq-company.github.io/acemq-java-amqp/security.html) — a user per service, permissions per name
