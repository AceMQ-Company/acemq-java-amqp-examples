package org.acemq.examples.intermediate;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.test.InMemoryTransport;

/**
 * The same code, with no broker anywhere.
 *
 * <p>{@code memory://orders} connects to an in-process broker. Publishing, consuming,
 * routing, acknowledgement and the failure policies all behave as they do over AMQP, and a
 * test that uses it runs in milliseconds instead of waiting for a container.
 *
 * <p>This is not a replacement for the integration tests. It is what makes the other
 * ninety per cent of your tests fast enough to run on every save.
 *
 * <p>No Docker needed: {@code mvn compile exec:java}.
 */
public final class TestingWithoutABroker {

    public record Order(String id, double total) { }

    public static void main(String[] args) throws Exception {
        long started = System.nanoTime();

        // The host names the broker instance. Two URLs with different hosts are two
        // separate brokers, which is how one test suite runs cases in parallel without
        // them seeing each other's queues.
        try (AceMq mq = AceMq.connect("memory://orders")) {
            mq.declareExchange("orders", "topic");
            // CLASSIC, spelled out. `declareQueue(name)` asks for a quorum queue, which
            // is the right default against RabbitMQ and something the in-memory transport
            // does not have -- so it refuses rather than quietly giving you a classic one.
            // That is the correct behaviour and it is also the one line of friction in
            // this example: code under test that declares its own queues needs the type
            // to be a parameter, not a hard-coded default.
            mq.declareQueue("orders.large", QueueType.CLASSIC, java.util.Map.of());
            mq.bind("orders.large", "orders", "order.placed");

            List<String> escalated = new CopyOnWriteArrayList<>();
            try (MessageConsumer consumer = mq.consume("orders.large", Order.class, message -> {
                        if (message.payload().total() > 1_000) {
                            escalated.add(message.payload().id());
                        }
                    })) {

                mq.publisher("orders", "order.placed", Order.class).send(new Order("small", 10.00));
                mq.publisher("orders", "order.placed", Order.class).send(new Order("large", 5_000.00));

                waitFor(() -> escalated.size() == 1, Duration.ofSeconds(5));
                System.out.printf("  escalated  %s%n", escalated);
            }
        }

        // Every test starts from nothing. Without this the second test in a class
        // inherits the first one's queues and passes for the wrong reason -- or fails
        // only when the suite is run in a different order.
        InMemoryTransport.reset();

        try (AceMq mq = AceMq.connect("memory://orders")) {
            // Not "empty" -- gone. Asking for the count of a queue that was discarded
            // fails, which is the difference between a reset and a purge and is worth
            // knowing when a test declares its topology in a @BeforeEach.
            long count = mq.messageCount("orders.large");
            System.out.printf("  after reset queue still exists, count=%d%n", count);
        } catch (RuntimeException e) {
            System.out.printf("  after reset the queue is gone: %s%n", e.getMessage());
        }

        // What it will not pretend to do. The in-memory transport declares which
        // capabilities it has, and streams are not among them: rather than quietly
        // behaving like a queue -- which would pass a test and fail in production -- it
        // refuses and says why.
        try (AceMq mq = AceMq.connect("memory://orders")) {
            mq.declareStream("orders.log", Duration.ofHours(1), 1_000_000L);
            System.out.println("  streams    unexpectedly supported");
        } catch (AceMqException e) {
            System.out.printf("  streams    refused: %s%n", firstSentence(e.getMessage()));
        }

        System.out.printf("  took       %d ms, no container started%n",
                Duration.ofNanos(System.nanoTime() - started).toMillis());
    }

    private static String firstSentence(String message) {
        int stop = message.indexOf(". ");
        return stop < 0 ? message : message.substring(0, stop);
    }

    private static void waitFor(java.util.function.BooleanSupplier done, Duration limit) throws Exception {
        long deadline = System.nanoTime() + limit.toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("timed out waiting for the example to progress");
            }
            Thread.sleep(10);
        }
    }
}
