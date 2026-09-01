package org.acemq.examples.basic;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;

/**
 * Writing one format and reading several.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class SerializationFormats {

    public record Order(String id, double total) { }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        try (AceMq mq = AceMq.connect(url)) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new");
            mq.bind("orders.new", "orders", "order.*");

            List<String> seen = new CopyOnWriteArrayList<>();

            // One consumer, and it is never told what format to expect. A consumer
            // reads every format on the classpath; that asymmetry is what turns a
            // format migration into two ordinary releases instead of a flag day.
            try (MessageConsumer consumer = mq.consume("orders.new", Order.class,
                    message -> seen.add(message.payload().id() + "=" + message.payload().total()))) {

                // JSON, because nobody said otherwise.
                mq.publisher("orders", "order.placed", Order.class)
                  .send(new Order("json", 1.00));

                // A publisher writes one format, chosen where the destination is
                // named. A queue carrying two formats at once is a queue nobody can
                // write a consumer against — so this is decided once, not per message.
                mq.publisher("orders", "order.placed", Order.class).asXml()
                  .send(new Order("xml", 2.00));

                mq.publisher("orders", "order.placed", Order.class).asYaml()
                  .send(new Order("yaml", 3.00));

                waitFor(() -> seen.size() == 3, Duration.ofSeconds(20));
            }

            seen.sort(String::compareTo);
            System.out.printf("  one consumer read all three: %s%n", seen);

            // Each publisher is a new object; asXml() does not mutate the one it was
            // called on. A long-lived publisher that quietly changed what it writes
            // would be worse than one that cannot.
            var json = mq.publisher("orders", "order.placed", Order.class);
            var xml = json.asXml();
            System.out.printf("  formats        json=%s xml=%s%n",
                    json.codec().contentType(), xml.codec().contentType());
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
