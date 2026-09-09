package org.acemq.examples.intermediate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.patterns.Scheduler;

/**
 * A message delivered later, with no scheduler process, no plugin and no cron.
 *
 * <p>The obvious implementation is a per-message time to live: drop the message in a queue nobody
 * consumes and let it dead-letter onwards. It is what most articles suggest and it is wrong,
 * because a classic queue expires messages only <strong>at its head</strong>. Put a four-hour
 * message in and a one-minute message behind it, and the one-minute message is delivered in four
 * hours with nothing reporting it.
 *
 * <p>What happens instead is a ladder of queues each with a <em>uniform</em> time to live —
 * {@code acemq.schedule.{1h,10m,1m,10s,1s}} — dead-lettering into {@code acemq.schedule.due},
 * where the scheduler either delivers the message or puts it in the largest rung that does not
 * overshoot. Every message in a rung has the same delay, so head-of-line expiry is harmless.
 *
 * <p>Read the two columns this prints together. A three-second delay lands at about two, because
 * a message ships as soon as less than a second is left rather than taking another hop for
 * accuracy nobody asked for. That is the trade, and it is stated rather than hidden: delivery is
 * accurate to about the smallest rung, and something that must fire at 09:00:00.000 wants a
 * scheduler rather than a message broker.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}. It takes about five
 * seconds, because it is waiting for real delays.
 */
public final class Scheduling {

    public record Reminder(String id, String askedFor) { }

    /** When it arrived, and what arrived. */
    private record Arrival(long nanos, Reminder reminder, String contentType) { }

    private static final String EXCHANGE = "reminders";
    private static final String QUEUE = "reminders.due";

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        try (AceMq mq = AceMq.connect(url)) {
            mq.declareExchange(EXCHANGE, "topic");
            mq.declareQueue(QUEUE);
            mq.bind(QUEUE, EXCHANGE, "reminder.#");

            List<Arrival> arrived = new CopyOnWriteArrayList<>();
            try (MessageConsumer ignored = mq.consume(QUEUE, Reminder.class, message -> arrived.add(
                    new Arrival(System.nanoTime(), message.payload(), message.contentType().orElse("<none>"))));
                    // Scheduler.on declares the exchange, the five rungs and the control queue.
                    // Every name and argument is shared with the Go, Python, Ruby and .NET
                    // libraries, because two services scheduling on one broker declare the same
                    // queues -- and a rung declared with a different time to live answers
                    // whichever declares second with PRECONDITION_FAILED.
                    Scheduler scheduler = Scheduler.on(mq)) {

                long started = System.nanoTime();

                // Anything already due is delivered at once rather than refused. A renewal date
                // that has gone by is a reminder that is late, not an error.
                scheduler.at(Instant.now().minusSeconds(60), EXCHANGE, "reminder.due",
                        new Reminder("R-0", "past"));
                scheduler.in(Duration.ofSeconds(3), EXCHANGE, "reminder.due",
                        new Reminder("R-1", "3s"));
                scheduler.in(Duration.ofSeconds(5), EXCHANGE, "reminder.due",
                        new Reminder("R-2", "5s"));

                waitFor(arrived, 3, Duration.ofSeconds(60));

                System.out.printf("  scheduled %d, delivered %d, hops %d%n",
                        scheduler.scheduled(), scheduler.delivered(), scheduler.hops());
                System.out.println();

                Map<String, Double> waited = new LinkedHashMap<>();
                List<Arrival> inOrder = new ArrayList<>(arrived);
                inOrder.sort(Comparator.comparingLong(Arrival::nanos));

                System.out.println("  asked for   arrived at");
                for (Arrival arrival : inOrder) {
                    double seconds = (arrival.nanos() - started) / 1_000_000_000.0;
                    waited.put(arrival.reminder().id(), seconds);
                    System.out.printf("  %9s   %6.1fs   %s%n",
                            arrival.reminder().askedFor(), seconds, arrival.reminder().id());
                }

                // The payload is encoded once, when it is scheduled, and carried as bytes from
                // then on. The content type travels in a header of its own and is put back on
                // the message finally delivered -- without that, what arrives is the right bytes
                // under application/octet-stream, and the consumer waiting for it cannot read
                // them.
                System.out.println();
                System.out.printf("  content type on arrival: %s%n", inOrder.get(0).contentType());

                require(scheduler.delivered() == 3,
                        "three messages went in, and " + scheduler.delivered() + " came out of the ladder");
                require(scheduler.hops() > 0,
                        "the delayed messages should have hopped through the rungs, hops=" + scheduler.hops());

                // The already-due one waited for nothing.
                require(waited.get("R-0") < 1.5,
                        "the past-dated reminder should have been delivered at once, and waited "
                                + waited.get("R-0") + "s");

                // And the delayed ones waited. This is the check worth having: a scheduler that
                // delivered everything immediately would satisfy every other assertion here. The
                // bounds are a second loose on purpose, for the reason the two columns show.
                require(waited.get("R-1") >= 1.5,
                        "the three-second reminder arrived after " + waited.get("R-1") + "s, which is too early");
                require(waited.get("R-2") >= 3.5,
                        "the five-second reminder arrived after " + waited.get("R-2") + "s, which is too early");
                require(waited.get("R-2") > waited.get("R-1"),
                        "the five-second reminder arrived before the three-second one");
                require("application/json".equals(inOrder.get(0).contentType()),
                        "the content type should have survived the ladder, and was "
                                + inOrder.get(0).contentType());
            }
        }
    }

    private static void waitFor(List<Arrival> arrived, int expected, Duration limit) throws Exception {
        long deadline = System.nanoTime() + limit.toNanos();
        while (arrived.size() < expected) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("expected " + expected + " reminders, saw " + arrived.size());
            }
            Thread.sleep(20);
        }
    }

    /** Fails the example rather than printing something untrue. */
    private static void require(boolean claim, String whatWasExpected) {
        if (!claim) {
            throw new IllegalStateException(whatWasExpected);
        }
    }
}
