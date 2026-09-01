package org.acemq.examples.basic;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.StreamConsumer;

/**
 * A log that is read rather than emptied.
 *
 * <p>A queue is destructive: one consumer takes a message and it is gone. A stream keeps
 * everything until retention removes it, every reader holds its own position, and the
 * same message can be read again tomorrow by somebody who did not exist today.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class StreamLog {

    public record OrderPlaced(String id, double total) { }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        try (AceMq mq = AceMq.connect(url)) {
            // A stream per run. Reading does not empty it, so a shared name would let
            // this run see everything every previous run wrote.
            String log = "orders.log." + UUID.randomUUID();

            // Retention is the argument that matters. Both limits may be null, and a
            // stream with neither grows until the disk is full — which on RabbitMQ is
            // not a stream problem but a broker-wide alarm that blocks every publisher
            // on the node.
            mq.declareStream(log, Duration.ofHours(1), 20L * 1024 * 1024);

            for (int i = 0; i < 10; i++) {
                mq.publisher("", log, OrderPlaced.class).send(new OrderPlaced("o-" + i, i));
            }
            System.out.printf("  wrote      10 orders to the log%n");

            // A projection being built for the first time. It has to see history, so it
            // says fromFirst(). A reader that is not told where to start reads from
            // fromNext() — right for a live consumer, silently wrong for this, which
            // would come up empty and look healthy.
            long checkpoint = firstFiveThenStop(mq, log);
            System.out.printf("  checkpoint offset %d, saved by the reader itself%n", checkpoint);

            // Resuming. The broker does not remember anybody's position — that is what
            // makes streams cheap — so the offset is the application's to store, next to
            // whatever the projection wrote.
            List<String> resumed = new CopyOnWriteArrayList<>();
            try (StreamConsumer consumer = mq.stream(log, OrderPlaced.class)
                    .fromOffset(checkpoint + 1)
                    .consume(message -> resumed.add(message.payload().id()))) {

                waitFor(() -> resumed.size() == 5, Duration.ofSeconds(60));
                System.out.printf("  resumed    %s%n", resumed);
            }

            // And the property a queue does not have: a second reader, attached now,
            // still sees everything. Nothing above consumed anything.
            List<String> auditor = new CopyOnWriteArrayList<>();
            try (StreamConsumer consumer = mq.stream(log, OrderPlaced.class)
                    .fromFirst()
                    .consume(message -> auditor.add(message.payload().id()))) {

                waitFor(() -> auditor.size() == 10, Duration.ofSeconds(60));
                System.out.printf("  a new reader still saw all %d%n", auditor.size());
            }
        }
    }

    /**
     * Reads five and stops, the way a process being restarted would.
     *
     * @return the offset of the last message it handled
     */
    private static long firstFiveThenStop(AceMq mq, String log) throws Exception {
        AtomicInteger handled = new AtomicInteger();
        // prefetch(1) so the stop is exact. With the default the broker has already
        // pushed more, and the reader would stop somewhere past where it says it did.
        try (StreamConsumer consumer = mq.stream(log, OrderPlaced.class)
                .fromFirst()
                .prefetch(1)
                .consume(message -> {
                    if (handled.get() == 5) {
                        throw new IllegalStateException("stopping, as a deployment would");
                    }
                    handled.incrementAndGet();
                })) {

            waitFor(() -> !consumer.isRunning(), Duration.ofSeconds(60));
            System.out.printf("  read       %d, then the reader stopped%n", handled.get());

            // The failure stopped the reader rather than skipping the message. On a
            // stream there is no dead-letter queue to move it to, and a projection with
            // a hole in it is wrong in a way nothing later notices.
            return consumer.lastHandledOffset().orElseThrow();
        }
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
