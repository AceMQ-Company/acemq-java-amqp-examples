package org.acemq.examples.advanced;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.acemq.amqp.api.PublishResult;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.QueueType;

/**
 * Three ways to publish a thousand messages, and why the fast one is not reckless.
 *
 * <p>{@code send} waits for the broker to confirm each message before returning. That is the
 * right default — it is the only version where a returned call means the broker has the
 * message — and it costs a network round trip per message, which is why bulk publishing
 * with it is slow in a way that looks like the broker's fault.
 *
 * <p>The alternative is not "give up confirms". It is to stop waiting for them one at a
 * time.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class Backpressure {

    public record Order(String id) { }

    private static final int BATCH = 1_000;

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        // maxOutstandingPublishes is the backpressure. It bounds how many messages may be
        // in flight without a confirm; the publisher blocks once the window is full rather
        // than queueing without limit in the client. That bound is what stops a fast
        // producer turning a slow broker into an OutOfMemoryError on the producer's side
        // -- the failure that looks like a client bug and is really an unbounded buffer.
        try (AceMq mq = AceMq.connect(ConnectionConfig.url(url)
                .maxOutstandingPublishes(256)
                .build())) {

            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, Map.of());
            mq.bind("orders.new", "orders", "order.placed");

            var publisher = mq.<Order>publisher("orders", "order.placed", Order.class);

            // One at a time, each confirmed before the next is sent.
            long oneByOne = time(() -> {
                for (int i = 0; i < BATCH; i++) {
                    publisher.send(new Order("sync-" + i));
                }
            });
            System.out.printf("  send        %d messages in %5d ms%n", BATCH, oneByOne);

            // Every message out, then every confirm awaited. Same guarantee at the end of
            // the call: nothing returns until the broker has acknowledged all of it.
            List<Order> batch = new ArrayList<>(BATCH);
            for (int i = 0; i < BATCH; i++) {
                batch.add(new Order("all-" + i));
            }
            long all = time(() -> publisher.sendAll(batch));
            System.out.printf("  sendAll     %d messages in %5d ms%n", BATCH, all);

            // Futures, for the caller who has something else to do meanwhile. The window
            // still applies -- this is asynchronous, not unbounded.
            long async = time(() -> {
                List<CompletableFuture<PublishResult>> pending = new ArrayList<>(BATCH);
                for (int i = 0; i < BATCH; i++) {
                    pending.add(publisher.sendAsync(new Order("async-" + i)));
                }
                // Waiting is not optional, it is only deferred. A future nobody joins is a
                // message nobody knows the fate of.
                CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).join();
            });
            System.out.printf("  sendAsync   %d messages in %5d ms%n", BATCH, async);

            System.out.printf("  speedup     sendAll %.0fx, sendAsync %.0fx%n",
                    oneByOne / (double) Math.max(all, 1), oneByOne / (double) Math.max(async, 1));
            System.out.printf("  queued      %d messages, all confirmed%n",
                    mq.messageCount("orders.new"));
        }
    }

    private static long time(ThrowingRunnable work) throws Exception {
        long started = System.nanoTime();
        work.run();
        return Duration.ofNanos(System.nanoTime() - started).toMillis();
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
