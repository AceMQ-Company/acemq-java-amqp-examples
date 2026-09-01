package org.acemq.examples.intermediate;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerGroup;

/**
 * Many consumers on one queue, and the two numbers that decide how it behaves.
 *
 * <p>Concurrency is how many handlers run at once. Prefetch is how many messages the
 * broker is allowed to hand a consumer before it acknowledges any of them. They are
 * confused constantly, and getting the second one wrong is what makes a queue with eight
 * consumers behave like a queue with one.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class ConsumerGroups {

    public record Order(String id) { }

    /** Long enough that the difference between serial and parallel is not a rounding error. */
    private static final Duration WORK = Duration.ofMillis(50);

    private static final int BATCH = 40;

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        try (AceMq mq = AceMq.connect(url)) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new");
            mq.bind("orders.new", "orders", "order.*");

            AtomicInteger handled = new AtomicInteger();
            // How many handlers were running at the same moment, at the busiest point.
            // Counting distinct thread names would not answer this: the group dispatches
            // onto a pool, so one consumer's handler runs on whichever thread is free and
            // a serial run still touches a dozen of them.
            AtomicInteger active = new AtomicInteger();
            AtomicInteger peak = new AtomicInteger();

            // prefetch(1) with a slow handler is the setting that distributes work
            // evenly: a consumer is given one message and gets another only once it has
            // finished. Raise it and the broker hands a batch to whoever asks first,
            // which is why a queue with idle consumers and a growing backlog is almost
            // always a prefetch problem rather than a scaling one.
            try (ConsumerGroup group = mq.consumeGroup("orders.new", Order.class, message -> {
                        peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                        try {
                            Thread.sleep(WORK.toMillis());
                            handled.incrementAndGet();
                        } finally {
                            active.decrementAndGet();
                        }
                    })
                    .concurrency(1)
                    .prefetch(1)
                    .start()) {

                System.out.printf("  group      size=%d prefetch=%d%n", group.size(), group.prefetch());

                long serial = timeBatch(mq, handled, BATCH);
                System.out.printf("  1 consumer  %d messages in %d ms, peak concurrency %d%n",
                        BATCH, serial, peak.get());

                // Scaling a running group. The consumers are added to the same queue on
                // the same connection; nothing is redeclared and nothing in flight is
                // disturbed.
                peak.set(0);
                group.scaleTo(8);
                System.out.printf("  scaled to  size=%d%n", group.size());

                long parallel = timeBatch(mq, handled, BATCH);
                System.out.printf("  8 consumers %d messages in %d ms, peak concurrency %d%n",
                        BATCH, parallel, peak.get());
                System.out.printf("  speedup    %.1fx%n", serial / (double) Math.max(parallel, 1));

                // Draining before closing: stop taking new messages, let the ones being
                // handled finish. Closing without this acknowledges nothing that was in
                // flight, and every one of those messages is redelivered to whoever
                // connects next -- correct, since they were never acknowledged, but a
                // surprise if you thought shutting down was free.
                boolean drained = group.drain(Duration.ofSeconds(30));
                System.out.printf("  drained    %s, inFlight=%d acknowledged=%d rejected=%d%n",
                        drained, group.inFlight(), group.acknowledged(), group.rejected());
            }

            System.out.printf("  handled    %d of %d, each exactly once%n", handled.get(), BATCH * 2);
        }
    }

    /** Publishes a batch and returns how many milliseconds it took to handle all of it. */
    private static long timeBatch(AceMq mq, AtomicInteger handled, int count) throws Exception {
        int before = handled.get();
        long started = System.nanoTime();
        for (int i = 0; i < count; i++) {
            mq.publisher("orders", "order.placed", Order.class).send(new Order("o-" + i));
        }
        waitFor(() -> handled.get() - before == count, Duration.ofSeconds(120));
        return Duration.ofNanos(System.nanoTime() - started).toMillis();
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
