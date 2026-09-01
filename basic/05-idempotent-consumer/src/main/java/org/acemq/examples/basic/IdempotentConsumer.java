package org.acemq.examples.basic;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.patterns.InMemoryIdempotencyStore;

/**
 * Handling a duplicate delivery once.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class IdempotentConsumer {

    public record Charge(String id, double amount) { }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        try (AceMq mq = AceMq.connect(url)) {
            mq.declareExchange("billing", "topic");
            mq.declareQueue("billing.charges");
            mq.bind("billing.charges", "billing", "charge.*");

            withoutAStore(mq);
            withAStore(mq);
        }
    }

    /** What at-least-once delivery costs when nothing is watching. */
    private static void withoutAStore(AceMq mq) throws Exception {
        AtomicInteger charged = new AtomicInteger();

        try (MessageConsumer consumer = mq.consume("billing.charges", Charge.class,
                ConsumerOptions.prefetch(1), message -> charged.incrementAndGet())) {

            sendTwice(mq, "c-1");
            waitFor(() -> charged.get() == 2, Duration.ofSeconds(15));
            System.out.printf("  no store       card charged %d times for one order%n", charged.get());
        }
    }

    /** The same two deliveries, handled once. */
    private static void withAStore(AceMq mq) throws Exception {
        AtomicInteger charged = new AtomicInteger();

        // Claim, then confirm on success or release on failure. Marking before the
        // handler runs would lose the work on a crash; marking after would let two
        // concurrent deliveries both pass the check.
        try (MessageConsumer consumer = mq.consume("billing.charges", Charge.class,
                ConsumerOptions.prefetch(1).idempotent(InMemoryIdempotencyStore.forOneDay()),
                message -> charged.incrementAndGet())) {

            sendTwice(mq, "c-2");
            waitFor(() -> consumer.duplicates() == 1, Duration.ofSeconds(15));
            // A moment for a second charge to arrive if the store were not working.
            // Asserting an absence needs time to fail to appear.
            Thread.sleep(1_000);

            System.out.printf("  with a store   card charged %d time, %d duplicate suppressed%n",
                    charged.get(), consumer.duplicates());
        }
    }

    /**
     * Publishes the same message twice, which is what a redelivery looks like from
     * the broker and what a retried publish looks like from an application. The
     * identifier is what makes them the same message rather than two.
     */
    private static void sendTwice(AceMq mq, String id) {
        Envelope envelope = Envelope.of("charge.taken").id(id).build();
        var publisher = mq.publisher("billing", "charge.taken", Charge.class);
        publisher.send(new Charge(id, 25.00), envelope);
        publisher.send(new Charge(id, 25.00), envelope);
    }

    private static void waitFor(java.util.function.BooleanSupplier done, Duration limit) throws Exception {
        long deadline = System.nanoTime() + limit.toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("timed out waiting for the example to progress");
            }
            Thread.sleep(100);
        }
    }
}
