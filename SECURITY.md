# Reporting a vulnerability

Email **security@acemq.com** with what you found and how to reproduce it. Please do
not open a public issue for anything exploitable.

You should get an acknowledgement within two working days, and an assessment of
whether it is a vulnerability, what is affected, and a rough timeline within a week.

## What this repository is

Runnable examples. Nothing here is meant to be a dependency of anything, and the code
is written to be read rather than deployed.

That does not make it exempt. An example is copied, and a bad pattern shown here ends
up in somebody's production service. So these are worth reporting:

- An example that disables TLS verification, trusts any certificate, or allows
  development certificates without saying plainly that it is doing so and why it is
  acceptable in that example.
- A credential that is real rather than obviously a placeholder.
- An example that acknowledges a message before the work is done, or swallows an
  error, without the surrounding text saying that is the point being illustrated.
- An example that would be insecure if copied into a service, presented as if it
  would not be.

## What is not

- **`guest:guest` against localhost.** That is RabbitMQ's default account, it only
  works over loopback, and these examples are meant to be run on a laptop.
- **Vulnerabilities in `acemq-java-amqp`** — report those against
  [that repository](https://github.com/AceMQ-Company/acemq-java-amqp).
- Findings from a scanner with no demonstrated impact.

## Supported versions

There are no releases. The main branch is what exists, and a fix is a commit on it.
