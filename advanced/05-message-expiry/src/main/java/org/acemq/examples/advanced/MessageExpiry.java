package org.acemq.examples.advanced;

import java.time.Duration;
import java.util.Map;

import org.acemq.amqp.api.Capability;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.PublishOptions;
import org.acemq.amqp.transport.QueueType;

/**
 * Messages that are worthless after a while, and the surprise in how they expire.
 *
 * <p>A price quote, a session token, a "still typing" notice: past a certain age, handling
 * it is worse than dropping it. A time to live says so on the message itself, so a
 * consumer that comes back after an outage does not work through an hour of stale
 * instructions.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class MessageExpiry {

    public record Quote(String id) { }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        try (AceMq mq = AceMq.connect(url)) {
            // Per-message expiry is a capability, not something every transport has.
            // Asking first is the difference between a documented limitation and a
            // mystery -- see advanced/04.
            System.out.printf("  supported  TTL_PER_MESSAGE=%s%n",
                    mq.supports(Capability.TTL_PER_MESSAGE));

            mq.declareExchange("quotes", "topic");
            // Expired messages are not simply deleted: with a dead-letter exchange on the
            // queue they are dead-lettered, which is what makes "how many did we drop"
            // answerable. Without one they vanish silently.
            mq.declareQueue("quotes.expired", QueueType.CLASSIC, Map.of());
            mq.declareQueue("quotes.new", QueueType.CLASSIC, Map.of(
                    "x-dead-letter-exchange", "",
                    "x-dead-letter-routing-key", "quotes.expired"));
            mq.bind("quotes.new", "quotes", "quote.*");

            var shortLived = mq.publisher("quotes", "quote.made", Quote.class)
                    .with(PublishOptions.defaults().expiringAfter(Duration.ofSeconds(1)));
            var durable = mq.publisher("quotes", "quote.made", Quote.class);

            shortLived.send(new Quote("q-1"));
            shortLived.send(new Quote("q-2"));
            System.out.printf("  published  2 quotes, each good for one second%n");
            System.out.printf("  queued     %d%n", mq.messageCount("quotes.new"));

            Thread.sleep(Duration.ofSeconds(3).toMillis());

            // Both are gone from the queue and both arrived in the dead-letter queue,
            // where they can be counted rather than merely missed.
            System.out.printf("  after 3s   queued=%d expired=%d%n",
                    mq.messageCount("quotes.new"), mq.messageCount("quotes.expired"));

            // The part that catches people. RabbitMQ expires a message when it reaches
            // the head of the queue, not on a timer -- so a short-lived message sitting
            // behind a long-lived one stays in the queue, counted, until the one in front
            // of it is dealt with.
            durable.send(new Quote("blocker"));
            shortLived.send(new Quote("q-3"));
            Thread.sleep(Duration.ofSeconds(3).toMillis());
            System.out.printf("  behind a   long-lived message: queued=%d (q-3 has expired but"
                    + " is still counted)%n", mq.messageCount("quotes.new"));

            // Reading the queue is what moves the head along, and the expired message is
            // dropped at that point rather than delivered.
            java.util.List<String> delivered = new java.util.concurrent.CopyOnWriteArrayList<>();
            try (var consumer = mq.consume("quotes.new", Quote.class,
                    message -> delivered.add(message.payload().id()))) {
                Thread.sleep(Duration.ofSeconds(2).toMillis());
            }
            System.out.printf("  delivered  %s — the expired one was dropped, not handled%n", delivered);
            System.out.printf("  after the  blocker is consumed: queued=%d expired=%d%n",
                    mq.messageCount("quotes.new"), mq.messageCount("quotes.expired"));
        }
    }
}
