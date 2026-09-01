package org.acemq.examples.intermediate;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;

/**
 * One trace across two brokers hops, so a graph shows the whole story.
 *
 * <p>Without propagation, an order that took nine seconds shows up as four unrelated
 * one-second spans in four services and five seconds nobody can account for. The trace
 * context travels on the message, and the consumer's span continues the publisher's trace
 * rather than starting its own.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class Tracing {

    public record OrderPlaced(String id) { }

    public record OrderShipped(String id, String tracking) { }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        TinyTracer tracer = new TinyTracer();
        AtomicInteger shipped = new AtomicInteger();

        // The tracer is given to the connection, not to each call. Every publish and
        // every handler is instrumented from here on, including the ones written later
        // by somebody who never heard of it.
        try (AceMq mq = AceMq.connect(url, tracer)) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.placed");
            mq.declareQueue("orders.shipped");
            mq.bind("orders.placed", "orders", "order.placed");
            mq.bind("orders.shipped", "orders", "order.shipped");

            try (MessageConsumer shipping = mq.consume("orders.placed", OrderPlaced.class,
                        message -> {
                            // A publish from inside a handler. It joins the trace of the
                            // message being handled, because the handler's scope is open
                            // on this thread while this runs -- which is what turns four
                            // services into one picture.
                            mq.publisher("orders", "order.shipped", OrderShipped.class)
                              .send(new OrderShipped(message.payload().id(), "TRK-1"));
                        });
                    MessageConsumer notifying = mq.consume("orders.shipped", OrderShipped.class,
                        message -> shipped.incrementAndGet())) {

                mq.publisher("orders", "order.placed", OrderPlaced.class).send(new OrderPlaced("o-1"));

                waitFor(() -> shipped.get() == 1 && tracer.finished().size() >= 4,
                        Duration.ofSeconds(30));
            }
        }

        List<TinyTracer.Span> spans = tracer.finished();
        Set<String> traces = spans.stream().map(TinyTracer.Span::traceId).collect(Collectors.toSet());

        System.out.printf("  spans      %d%n", spans.size());
        System.out.printf("  traces     %d%n", traces.size());
        for (TinyTracer.Span span : spans) {
            System.out.printf("    %-24s parent=%s%n", span,
                    span.parentId().isEmpty() ? "(root)" : span.parentId());
        }

        // Four operations across two hops, and one trace. That single number is the
        // difference between a graph that explains a latency spike and four that do not.
        System.out.printf("  one trace  %s%n", traces.size() == 1);
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
