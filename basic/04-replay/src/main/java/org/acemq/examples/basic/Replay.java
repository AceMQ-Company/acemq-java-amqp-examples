package org.acemq.examples.basic;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;

/**
 * Getting messages back out of a dead-letter queue once the cause is fixed.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class Replay {

    public record Order(String id, double total) { }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        try (AceMq mq = AceMq.connect(url)) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new");
            mq.bind("orders.new", "orders", "order.*");

            RetryPolicy policy = RetryPolicy.fixed(2, Duration.ofSeconds(1)).withJitter(0);
            AtomicBoolean downstreamIsBroken = new AtomicBoolean(true);
            List<Envelope> handled = new CopyOnWriteArrayList<>();

            // A consumer that fails while the downstream is down, and works once it
            // is back. The same consumer stays up throughout: the fix is a deployment
            // in real life, and the messages waiting in the dead-letter queue do not
            // care which.
            try (MessageConsumer consumer = mq.consume("orders.new", Order.class,
                    ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                        if (downstreamIsBroken.get()) {
                            throw new IllegalStateException("the inventory service is down");
                        }
                        handled.add(message.envelope());
                    })) {

                for (int i = 1; i <= 3; i++) {
                    mq.publisher("orders", "order.placed", Order.class)
                      .send(new Order("o-" + i, i * 10.0));
                }

                waitUntil(() -> consumer.deadLettered() == 3, Duration.ofSeconds(90));
                System.out.printf("  dead-lettered  %d orders while the service was down%n",
                        consumer.deadLettered());

                // Look before touching. Replaying forty thousand messages into a
                // consumer that is still behind is a decision, and this is the
                // number that decision needs.
                org.acemq.amqp.core.Replay replay = mq.replay("orders.new");
                System.out.printf("  waiting        %d in %s%n", replay.pending(), replay.from());

                // The fix.
                downstreamIsBroken.set(false);

                // A bounded batch first, which is what you do in production when you
                // are not yet sure the fix holds.
                int firstBatch = replay.replay(1);
                waitUntil(() -> handled.size() == 1, Duration.ofSeconds(30));
                System.out.printf("  replayed       %d as a trial, %d handled%n", firstBatch, handled.size());

                int rest = replay.replayAll();
                waitUntil(() -> handled.size() == 3, Duration.ofSeconds(30));
                System.out.printf("  replayed       %d more, %d handled in total%n", rest, handled.size());

                Envelope first = handled.get(0);
                System.out.printf("  provenance     replayedFrom=%s replayCount=%d attempt=%d%n",
                        first.replayedFrom().orElse("-"), first.replayCount(), first.attempt());
                System.out.printf("  why it failed  %s%n", first.error().orElse("-"));
                System.out.printf("  dlq now        %d%n", mq.messageCount("orders.new.dlq"));
            }
        }
    }

    private static void waitUntil(java.util.function.BooleanSupplier done, Duration limit) throws Exception {
        long deadline = System.nanoTime() + limit.toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("timed out waiting for the example to progress");
            }
            Thread.sleep(200);
        }
    }
}
