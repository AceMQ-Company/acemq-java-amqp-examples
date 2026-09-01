package org.acemq.examples.basic;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.Pipeline;

/**
 * Three steps, three queues.
 *
 * <p>Chaining handlers inside one process is function composition wearing a messaging
 * costume: a crash loses the work, a slow step blocks the chain, and scaling is all or
 * nothing. With a queue between each pair, a crash leaves the message where it was, a slow
 * step grows its own queue while the others carry on, and the step that needs eight
 * consumers gets eight while its neighbour keeps one.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class OrderPipeline {

    public record Order(String id, double total) { }

    public record Priced(String id, double total, double tax) { }

    public record Dispatched(String id, String tracking) { }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        try (AceMq mq = AceMq.connect(url)) {
            List<String> dispatched = new CopyOnWriteArrayList<>();
            List<String> rejected = new CopyOnWriteArrayList<>();

            // The type moves along the chain: a step producing a Priced cannot be put
            // where one producing an Order belongs, and the compiler says so rather than
            // the broker doing it at three in the morning.
            try (Pipeline<Order> fulfilment = mq.pipeline("fulfilment", Order.class)

                    // Returning null ends the route. A filtering step is one that
                    // sometimes returns nothing: the message is acknowledged, nothing
                    // downstream sees it, and it is counted apart from both success and
                    // failure — a rejected order is a decision, not an error.
                    .step("validate", Order.class, message -> {
                        Order order = message.payload();
                        if (order.total() <= 0) {
                            rejected.add(order.id());
                            return null;
                        }
                        return order;
                    })

                    // Retries are configured per step, because the reasons differ. A tax
                    // service that times out is worth retrying; a validation rule is not,
                    // and retrying it just fails four more times.
                    .step("price", Priced.class, message -> {
                        Order order = message.payload();
                        return new Priced(order.id(), order.total(), order.total() * 0.2);
                    })
                    .withRetry(RetryPolicy.exponential(3, Duration.ofSeconds(1), Duration.ofSeconds(10)))

                    // And concurrency per step: this is the slow one, so it gets four
                    // consumers while the others keep one. On a single chained handler
                    // that choice does not exist.
                    .step("dispatch", Dispatched.class, message -> {
                        Priced priced = message.payload();
                        Thread.sleep(50);
                        Dispatched result = new Dispatched(priced.id(), "TRK-" + priced.id());
                        dispatched.add(result.tracking());
                        return result;
                    })
                    .concurrency(4)

                    .build()) {

                System.out.printf("  steps      %s%n", fulfilment.stepNames());
                System.out.printf("  queues     %s%n", fulfilment.stepNames().stream()
                        .map(fulfilment::queueFor).toList());

                for (int i = 1; i <= 8; i++) {
                    fulfilment.send(new Order("o-" + i, i * 10.0));
                }
                // The one that will not survive validation.
                fulfilment.send(new Order("o-free", 0.0));

                waitFor(() -> fulfilment.completed() + fulfilment.endedEarly() == 9,
                        Duration.ofSeconds(60));

                dispatched.sort(String::compareTo);
                System.out.printf("  dispatched %s%n", dispatched);
                System.out.printf("  rejected   %s at validate, and never priced%n", rejected);
                System.out.printf("  counts     entered=%d completed=%d endedEarly=%d%n",
                        fulfilment.entered(), fulfilment.completed(), fulfilment.endedEarly());
            }
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
