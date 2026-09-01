package org.acemq.examples.advanced;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Capability;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.test.InMemoryTransport;
import org.acemq.amqp.transport.QueueType;

/**
 * Code that works against two transports without pretending they are the same.
 *
 * <p>A library spanning several brokers has two dishonest options and one honest one. It
 * can expose the lowest common denominator, which throws away the reason people chose
 * RabbitMQ. It can pretend everything is supported and fail at run time in production. Or
 * it can say what each transport can do and let the application decide.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java} — or run it with no
 * broker at all and it reports on the in-memory transport alone.
 */
public final class Portability {

    public record Order(String id) { }

    public static void main(String[] args) throws Exception {
        InMemoryTransport.reset();

        report("memory://orders");

        // The same code against a real broker, if one is there. Nothing below is
        // conditional on which transport it is -- only on what that transport says it can
        // do, which is the difference between portable code and code with an if-rabbitmq
        // in it.
        String url = args.length > 0 ? args[0] : System.getenv("AMQP_URL");
        if (url == null) {
            System.out.printf("  (no AMQP_URL, so only the in-memory transport was examined)%n");
            return;
        }
        report(url);
    }

    private static void report(String url) throws Exception {
        try (AceMq mq = AceMq.connect(url)) {
            Set<Capability> has = mq.capabilities();
            Set<String> missing = new TreeSet<>();
            for (Capability capability : EnumSet.allOf(Capability.class)) {
                if (!has.contains(capability)) {
                    missing.add(capability.name());
                }
            }
            System.out.printf("%n  === %s ===%n", url.replaceAll("://.*@", "://"));
            System.out.printf("  supports   %d of %d capabilities%n",
                    has.size(), Capability.values().length);
            System.out.printf("  missing    %s%n", missing.isEmpty() ? "(nothing)" : missing);

            // Asking before doing. This is the shape that ports cleanly: the feature is
            // used where it exists and a documented alternative is used where it does
            // not, decided by the transport rather than by a comment.
            String queue = "orders.durable";
            if (mq.supports(Capability.QUORUM_QUEUES)) {
                mq.declareQueue(queue);
                System.out.printf("  queue      quorum, replicated%n");
            } else {
                mq.declareQueue(queue, QueueType.CLASSIC, Map.of());
                System.out.printf("  queue      classic — this transport has no quorum queues,%n");
                System.out.printf("             so durability is whatever the single node gives us%n");
            }

            // And what happens when you skip the check. The library refuses rather than
            // quietly substituting something that behaves differently, which is the
            // failure you want: at declare time, on a laptop, not at three in the morning.
            if (!mq.supports(Capability.STREAMS)) {
                try {
                    mq.declareStream("orders.log", Duration.ofHours(1), 1_000_000L);
                    System.out.printf("  streams    unexpectedly declared%n");
                } catch (AceMqException e) {
                    System.out.printf("  streams    refused, and says which transport and why%n");
                }
            } else {
                System.out.printf("  streams    available%n");
            }
        }
    }
}
