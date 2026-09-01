package org.acemq.examples.intermediate;

import java.time.Duration;
import java.util.Map;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.test.InMemoryTransport;
import org.acemq.amqp.transport.ConnectionBlockedException;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.QueueType;

/**
 * What happens when the broker runs out of disk, and why nobody notices in time.
 *
 * <p>RabbitMQ raises an alarm when memory or disk crosses a watermark and stops accepting
 * publishes. It does not close the connection and it does not fail anything already sent —
 * publishers simply stop being served. A service that publishes once a minute discovers
 * this a minute later; one that is idle does not discover it at all.
 *
 * <p>Reproducing a real alarm means filling a disk, so this runs against the in-memory
 * transport, which can be told to block on demand. The behaviour it models is the real
 * one.
 *
 * <p>No Docker needed: {@code mvn compile exec:java}.
 */
public final class BlockedConnections {

    public record Order(String id) { }

    public static void main(String[] args) throws Exception {
        InMemoryTransport.reset();

        // blockedTimeout is how long a publish waits for the alarm to clear before
        // giving up. The default is thirty seconds, which is a reasonable bet that a
        // brief alarm will pass -- and is also thirty seconds of a request thread doing
        // nothing, per message, for as long as the alarm lasts. Whatever you choose,
        // choose it: the default decides how your service behaves under an alarm, and
        // "the whole pool is parked on publish" is how one broker running low on disk
        // takes an application down with it.
        try (AceMq mq = AceMq.connect(ConnectionConfig.url("memory://orders")
                .blockedTimeout(Duration.ofSeconds(2))
                .build())) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, Map.of());
            mq.bind("orders.new", "orders", "order.*");

            mq.publisher("orders", "order.placed", Order.class).send(new Order("o-1"));
            System.out.printf("  before     published, blocked=%s%n", mq.isBlocked());

            // The broker crosses a watermark. Nothing is sent to the client at this
            // point in the general case, and nothing that was already published is
            // affected.
            InMemoryTransport.block("orders", "low on disk");
            System.out.printf("  alarm      raised, isBlocked=%s reason=%s%n",
                    mq.isBlocked(), mq.blockedReason().orElse("(none)"));

            // The next publish is where it surfaces. This is the important part: an
            // idle connection is told nothing, so "are we blocked" cannot be answered by
            // waiting for an event -- a publisher finds out by publishing.
            long waitedFrom = System.nanoTime();
            try {
                mq.publisher("orders", "order.placed", Order.class).send(new Order("o-2"));
                System.out.println("  during     unexpectedly published while blocked");
            } catch (ConnectionBlockedException e) {
                // Two questions worth asking of the exception. The reason is the broker's
                // own words, which is what belongs in the alert -- "low on disk" is
                // actionable and "publish failed" is not.
                System.out.printf("  during     refused: reason=%s%n", e.reason());
                // And whether the message might still have reached the broker. A publish
                // rejected before it left is safe to retry; one that may have landed is
                // a duplicate risk, and the difference decides whether a retry needs an
                // idempotency key.
                System.out.printf("  during     mayHaveBeenPublished=%s%n", e.mayHaveBeenPublished());
                // How long the call held the caller's thread. This is the number that
                // decides whether an alarm degrades the service or stops it.
                System.out.printf("  during     the publish waited about %d s%n",
                        Duration.ofNanos(System.nanoTime() - waitedFrom).toSeconds());
            }

            // Consumers keep working throughout. An alarm stops publishers so the queues
            // can drain, which is the whole point of it -- a service that shuts its
            // consumers down on a publish failure removes the only thing that would clear
            // the alarm.
            System.out.printf("  meanwhile  the queue still holds %d, consumers unaffected%n",
                    mq.messageCount("orders.new"));

            InMemoryTransport.unblock("orders");
            mq.publisher("orders", "order.placed", Order.class).send(new Order("o-3"));
            System.out.printf("  after      cleared, published again, blocked=%s%n", mq.isBlocked());
            System.out.printf("  queue      holds %d%n", mq.messageCount("orders.new"));
        }
    }
}
