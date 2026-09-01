package org.acemq.examples.basic;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.api.PublishResult;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;

/**
 * The smallest useful thing: publish a message, have the broker confirm it, consume it.
 *
 * <p>Run it with {@code docker compose up -d} and {@code mvn compile exec:java}.
 */
public final class PublishAndConfirm {

    /** What we are sending. An ordinary record; nothing here knows about messaging. */
    public record Order(String id, String customer, double total) { }

    public static void main(String[] args) throws Exception {
        // An argument first, then the environment, then the obvious default. The
        // argument is what lets the test point this at a throwaway broker without
        // the example needing to know it is being tested.
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        // AceMq is the connection. Closing it closes every publisher and consumer
        // created from it, so try-with-resources cannot leak a subscription.
        try (AceMq mq = AceMq.connect(url)) {

            // Declaring is idempotent: this is safe to run at every start-up.
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new");
            mq.bind("orders.new", "orders", "order.*");

            CountDownLatch handled = new CountDownLatch(1);

            // The handler runs on threads the library owns. The message is acknowledged
            // after it returns -- not when it was delivered -- so a handler that throws,
            // or a process that dies mid-handler, does not lose the message.
            try (MessageConsumer consumer = mq.consume("orders.new", Order.class, message -> {
                Order order = message.payload();
                System.out.printf("  consumed  %s for %s, %.2f%n",
                        order.id(), order.customer(), order.total());
                handled.countDown();
            })) {

                // send() does not return until RabbitMQ has taken responsibility for the
                // message. The payload becomes JSON because nobody said otherwise.
                PublishResult result = mq.publisher("orders", "order.placed", Order.class)
                        .send(new Order("o-1001", "acme", 42.00));

                System.out.printf("  published %s, confirmed in %s%n",
                        result.messageId(), result.latency());

                if (!handled.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("the message was never delivered");
                }
            }

            // The other half of "no message disappears quietly": nothing is bound for
            // this routing key, so the publish fails rather than being silently dropped.
            // A typo in a routing key is the easiest way to lose every message you send.
            try {
                mq.publisher("orders", "nobody.listens", Order.class)
                        .send(new Order("o-1002", "acme", 7.00));
                System.out.println("  unreachable: an unroutable publish should have thrown");
            } catch (PublishFailedException expected) {
                System.out.println("  refused   an unroutable message, as it should");
            }
        }
    }
}
