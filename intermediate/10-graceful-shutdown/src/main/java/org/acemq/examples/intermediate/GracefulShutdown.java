package org.acemq.examples.intermediate;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.test.InMemoryTransport;
import org.acemq.amqp.transport.QueueType;

/**
 * What happens to the message being handled when the pod is told to stop.
 *
 * <p>Kubernetes sends SIGTERM and starts a clock. When it runs out the process is killed,
 * and anything still in a handler dies with it. Those messages were never acknowledged, so
 * the broker redelivers them — correct, and the reason a deployment shows up as a spike of
 * duplicate work if nobody arranged otherwise.
 *
 * <p>The two calls do different things and the difference is the whole example.
 * {@code close()} cancels the subscription and returns; it does not wait for a handler
 * that is still running. {@code drain(timeout)} cancels, then waits for the handlers to
 * finish, and <em>tells you whether they did</em>.
 *
 * <p>No Docker needed: {@code mvn compile exec:java}.
 */
public final class GracefulShutdown {

    public record Order(String id) { }

    /** Long enough that a message is genuinely still being handled at shutdown. */
    private static final Duration WORK = Duration.ofMillis(300);

    public static void main(String[] args) throws Exception {
        // Enough time for the handler in hand to finish. This is the shape a service
        // wants: SIGTERM, drain within the grace period, then close.
        shutdown("enough time", Duration.ofSeconds(10));

        // The same shutdown with a grace period shorter than the handler. drain returns
        // false, which is the signal worth logging and alerting on: it means messages
        // were abandoned mid-flight and will be redelivered to whoever starts next.
        shutdown("not enough", Duration.ofMillis(50));

        // And no drain at all, for contrast: close() does not wait, so whether the
        // handler finishes is a race with process exit.
        shutdown("no drain", null);
    }

    private static void shutdown(String label, Duration grace) throws Exception {
        InMemoryTransport.reset();

        try (AceMq mq = AceMq.connect("memory://orders")) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, Map.of());
            mq.bind("orders.new", "orders", "order.*");

            AtomicInteger handled = new AtomicInteger();

            MessageConsumer consumer = mq.consume("orders.new", Order.class,
                    ConsumerOptions.prefetch(5),
                    message -> {
                        Thread.sleep(WORK.toMillis());
                        handled.incrementAndGet();
                    });

            for (int i = 1; i <= 10; i++) {
                mq.publisher("orders", "order.placed", Order.class).send(new Order("o-" + i));
            }

            // SIGTERM arrives here, with a message half handled.
            Thread.sleep(WORK.toMillis() / 2);

            if (grace == null) {
                consumer.close();
                System.out.printf("  %-11s closed without draining%n", label);
            } else {
                boolean quiet = consumer.drain(grace);
                System.out.printf("  %-11s drained=%s inFlight=%d%n", label, quiet, consumer.inFlight());
                consumer.close();
            }

            System.out.printf("  %-11s handled=%d acknowledged=%d, %d left on the queue%n",
                    label, handled.get(), consumer.acknowledged(), mq.messageCount("orders.new"));
        }
    }
}
