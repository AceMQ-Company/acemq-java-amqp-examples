package org.acemq.examples.advanced;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Topology;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.test.InMemoryTransport;

/**
 * One broker, several customers, and the question of what happens when one of them
 * misbehaves.
 *
 * <p>The cheap answer is a tenant field in the payload and one shared queue. It works until
 * a customer sends ten million messages, or a poison message from one tenant stops the
 * consumer that serves all of them, or somebody has to prove to an auditor that tenant A's
 * data was never readable by tenant B.
 *
 * <p>A queue per tenant costs a little topology and answers all three.
 *
 * <p>No Docker needed: {@code mvn compile exec:java}.
 */
public final class MultiTenantTopology {

    public record Order(String id) { }

    private static final List<String> TENANTS = List.of("acme", "globex", "initech");

    public static void main(String[] args) throws Exception {
        InMemoryTransport.reset();

        try (AceMq mq = AceMq.connect("memory://orders")) {
            // The topology is generated from the tenant list rather than written out
            // three times. Adding a customer is a list entry, and every tenant is
            // therefore configured identically -- the drift you get from copy-paste is
            // where "why does tenant C have no dead-letter queue" comes from.
            Topology.Builder builder = Topology.define().exchange("orders", "topic");
            for (String tenant : TENANTS) {
                builder.classicQueue(queueFor(tenant), Map.of())
                       .bind(queueFor(tenant), "orders", "order." + tenant + ".*");
            }
            Topology topology = builder.build();

            mq.topology().apply(topology, ApplyMode.CREATE_ONLY);
            System.out.printf("  topology   %d queues, one per tenant%n", TENANTS.size());

            // The routing key carries the tenant, so the broker does the separation. A
            // consumer never has to filter, and cannot forget to.
            List<String> acmeSaw = new CopyOnWriteArrayList<>();
            try (MessageConsumer consumer = mq.consume(queueFor("acme"), Order.class,
                    message -> acmeSaw.add(message.payload().id()))) {

                mq.publisher("orders", "order.acme.placed", Order.class).send(new Order("a-1"));
                mq.publisher("orders", "order.globex.placed", Order.class).send(new Order("g-1"));
                mq.publisher("orders", "order.initech.placed", Order.class).send(new Order("i-1"));

                waitFor(() -> acmeSaw.size() == 1, Duration.ofSeconds(10));
            }

            System.out.printf("  acme saw   %s%n", acmeSaw);
            for (String tenant : TENANTS) {
                System.out.printf("  %-10s queue holds %d%n", tenant, mq.messageCount(queueFor(tenant)));
            }

            // The blast radius. One tenant floods; the others are untouched, because the
            // depth that matters is per queue rather than shared. On one queue this is
            // the incident where every customer is slow because of one of them.
            for (int i = 0; i < 500; i++) {
                mq.publisher("orders", "order.globex.placed", Order.class).send(new Order("g-" + i));
            }
            System.out.printf("  after a flood by globex:%n");
            for (String tenant : TENANTS) {
                System.out.printf("  %-10s queue holds %d%n", tenant, mq.messageCount(queueFor(tenant)));
            }

            // And the operational consequence worth stating: this is also the unit you
            // can pause, drain, rate-limit or move to another node. None of those are
            // available when every tenant shares a queue.
            System.out.printf("  isolation  each tenant is a queue: its own depth, its own"
                    + " dead-letter queue, its own consumers%n");
        }
    }

    /**
     * Tenant in the name, not in the payload.
     *
     * <p>A permission in RabbitMQ is a regular expression over names, so {@code orders.acme.*}
     * is grantable and "the messages where the tenant field says acme" is not. Naming is what
     * makes per-tenant credentials possible at all.
     */
    private static String queueFor(String tenant) {
        return "orders." + tenant;
    }

    private static void waitFor(java.util.function.BooleanSupplier done, Duration limit) throws Exception {
        long deadline = System.nanoTime() + limit.toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("timed out waiting for the example to progress");
            }
            Thread.sleep(20);
        }
    }
}
