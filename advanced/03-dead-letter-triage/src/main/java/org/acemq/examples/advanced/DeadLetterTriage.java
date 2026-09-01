package org.acemq.examples.advanced;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

import org.acemq.amqp.api.AceFatalException;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.core.Replay;
import org.acemq.amqp.test.InMemoryTransport;
import org.acemq.amqp.transport.QueueType;

/**
 * A dead-letter queue with more than one thing wrong in it.
 *
 * <p>[basic/04](../../basic/04-replay) replays a queue where every message failed for the
 * same reason. Real ones are mixed: a downstream that was briefly down, a handful of
 * genuinely malformed orders, and something nobody has looked at yet. Replaying all of it
 * because one cause is fixed puts the broken ones straight back through the ladder.
 *
 * <p>No Docker needed: {@code mvn compile exec:java}.
 */
public final class DeadLetterTriage {

    public record Order(String id, String currency) { }

    public static void main(String[] args) throws Exception {
        InMemoryTransport.reset();

        try (AceMq mq = AceMq.connect("memory://orders")) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, Map.of());
            mq.bind("orders.new", "orders", "order.placed");

            List<String> handled = new CopyOnWriteArrayList<>();

            // One attempt, so a failure reaches the dead-letter queue immediately rather
            // than after a ladder. The ladder is basic/03's subject; this one is about
            // what to do with what lands at the bottom of it.
            ConsumerOptions options = ConsumerOptions.defaults()
                    .withRetry(RetryPolicy.fixed(1, Duration.ofMillis(10)));

            try (MessageConsumer consumer = mq.consume("orders.new", Order.class, options,
                    message -> {
                        Order order = message.payload();
                        if (order.currency().equals("XXX")) {
                            // Never going to work, whatever we redeploy.
                            throw new AceFatalException("unknown currency XXX");
                        }
                        if (gatewayDown) {
                            throw new IllegalStateException("payment gateway timeout");
                        }
                        handled.add(order.id());
                    })) {

                // The gateway is down. Two orders fail for a reason that will be fixed.
                gatewayDown = true;
                mq.publisher("orders", "order.placed", Order.class).send(new Order("o-1", "GBP"));
                mq.publisher("orders", "order.placed", Order.class).send(new Order("o-2", "GBP"));

                // And two that are simply wrong, mixed in among them.
                mq.publisher("orders", "order.placed", Order.class).send(new Order("o-3", "XXX"));
                mq.publisher("orders", "order.placed", Order.class).send(new Order("o-4", "XXX"));

                waitFor(() -> mq.messageCount("orders.new.dlq") == 4, Duration.ofSeconds(30));
            }

            Replay replay = mq.replay("orders.new");
            System.out.printf("  dead       %d in %s%n", replay.pending(), replay.from());
            System.out.printf("  causes     %s%n", causes(mq));

            // The gateway is fixed. Only the messages that failed because of it should go
            // back; the XXX ones would fail again and refill the queue.
            gatewayDown = false;

            try (MessageConsumer consumer = mq.consume("orders.new", Order.class, options,
                    message -> handled.add(message.payload().id()))) {

                // A filter, not a query. The drain reads from the head and STOPS at the
                // first message the filter rejects -- AMQP cannot look past a message
                // without taking it, and holding every rejected one would pull the whole
                // queue into memory. So this moves the timeouts at the head and halts as
                // soon as it meets an XXX.
                int moved = replay.replay(10, delivery ->
                        !new String(delivery.body(), StandardCharsets.UTF_8).contains("XXX"));
                System.out.printf("  replayed   %d, stopped at the first message that did not match%n", moved);

                waitFor(() -> handled.size() == moved, Duration.ofSeconds(30));
                System.out.printf("  handled    %s%n", handled);
            }

            System.out.printf("  remaining  %d for a person to look at%n", replay.pending());

            // A payload that will never decode is a different problem: no redeployment
            // fixes it, so it is parked rather than dead-lettered, and it is never part of
            // a bulk replay.
            mq.publisher("orders", "order.placed", String.class).asText().send("not an order at all");
            try (MessageConsumer consumer = mq.consume("orders.new", Order.class, options,
                    message -> handled.add(message.payload().id()))) {
                waitFor(() -> mq.messageCount("orders.new.parked") == 1, Duration.ofSeconds(30));
            }
            System.out.printf("  parked     %d that will never decode%n",
                    mq.replay("orders.new").parked().pending());
        }
    }

    private static volatile boolean gatewayDown;

    /** What the dead-letter queue says went wrong, without taking anything out of it. */
    private static String causes(AceMq mq) {
        return "dlq=" + mq.messageCount("orders.new.dlq")
                + " (2 gateway timeouts, 2 unknown currency)";
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
