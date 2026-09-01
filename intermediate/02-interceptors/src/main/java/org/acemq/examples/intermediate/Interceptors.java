package org.acemq.examples.intermediate;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.Ack;
import org.acemq.amqp.api.ConsumeContext;
import org.acemq.amqp.api.ConsumeInterceptor;
import org.acemq.amqp.api.PublishContext;
import org.acemq.amqp.api.PublishInterceptor;
import org.acemq.amqp.api.PublishResult;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;

/**
 * The cross-cutting things nobody should have to remember at every call site.
 *
 * <p>Stamping a tenant on every message, timing every handler, counting failures: written
 * by hand these are five lines repeated in forty places, and the bug is always the place
 * somebody forgot. An interceptor is registered once on the connection and applies to
 * everything that goes through it.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class Interceptors {

    public record Order(String id, double total) { }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        List<String> events = new CopyOnWriteArrayList<>();
        AtomicInteger handled = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();

        try (AceMq mq = AceMq.connect(url)) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new");
            mq.bind("orders.new", "orders", "order.*");

            // Adds a header to every message published on this connection. The publisher
            // call sites below never mention a tenant, and they cannot forget to.
            mq.intercept(new PublishInterceptor() {
                @Override
                public PublishContext beforePublish(PublishContext context) {
                    events.add("stamp:" + context.routingKey());
                    // The context is replaced rather than mutated: an interceptor that
                    // edited the envelope in place would change a message another
                    // interceptor had already seen.
                    return context.withEnvelope(context.envelope().toBuilder()
                            .header("x-tenant", "acme")
                            .build());
                }

                @Override
                public void afterConfirm(PublishContext context, PublishResult result) {
                    events.add("confirmed:routed=" + result.routed());
                }

                @Override
                public int order() {
                    // Lower runs first. Stamping must happen before anything that reads
                    // the header, and leaving that to registration order is how it breaks
                    // the day somebody reorders two lines of start-up code.
                    return 10;
                }
            });

            // A second one, to show the ordering is the number and not the registration.
            mq.intercept(new PublishInterceptor() {
                @Override
                public PublishContext beforePublish(PublishContext context) {
                    events.add("audit:tenant=" + context.envelope().headers().get("x-tenant"));
                    return context;
                }

                @Override
                public int order() {
                    return 20;
                }
            });

            mq.intercept(new ConsumeInterceptor() {
                @Override
                public void beforeHandle(ConsumeContext context) {
                    events.add("received:tenant=" + context.envelope().headers().get("x-tenant"));
                }

                @Override
                public void afterHandle(ConsumeContext context, Ack ack) {
                    // The Ack says what the library decided to do with the message, which
                    // is the number worth graphing: accepted, retried, dead-lettered.
                    events.add("settled:accept=" + ack.isAccept());
                }

                @Override
                public void onError(ConsumeContext context, Throwable failure) {
                    // Called when the handler threw, before the failure policy runs. The
                    // one place to hang error reporting without wrapping every handler in
                    // try/catch.
                    failures.incrementAndGet();
                    events.add("failed:" + failure.getMessage());
                }
            });

            try (MessageConsumer consumer = mq.consume("orders.new", Order.class, message -> {
                        if (message.payload().total() < 0) {
                            throw new IllegalStateException("negative total");
                        }
                        handled.incrementAndGet();
                    })) {

                mq.publisher("orders", "order.placed", Order.class).send(new Order("o-1", 42.00));
                mq.publisher("orders", "order.placed", Order.class).send(new Order("o-bad", -1.00));

                waitFor(() -> handled.get() == 1 && failures.get() >= 1, Duration.ofSeconds(30));
            }

            System.out.printf("  handled    %d, failed %d%n", handled.get(), failures.get());
            System.out.printf("  publish    %s%n", events.stream()
                    .filter(e -> e.startsWith("stamp:") || e.startsWith("audit:"))
                    .distinct().toList());
            System.out.printf("  consume    %s%n", events.stream()
                    .filter(e -> e.startsWith("received:") || e.startsWith("failed:"))
                    .distinct().toList());
            System.out.printf("  confirms   %s%n", events.stream()
                    .filter(e -> e.startsWith("confirmed:")).distinct().toList());
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier done, Duration limit) throws Exception {
        long deadline = System.nanoTime() + limit.toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("timed out waiting for the example to progress");
            }
            Thread.sleep(50);
        }
    }
}
