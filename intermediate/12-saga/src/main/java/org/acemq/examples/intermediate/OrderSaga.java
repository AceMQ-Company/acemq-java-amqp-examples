package org.acemq.examples.intermediate;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.patterns.Saga;
import org.acemq.amqp.patterns.SagaResult;

/**
 * Three services, no shared transaction, and what happens when the third one says no.
 *
 * <p>Reserving stock, taking a payment and booking a courier are three systems with three
 * databases. There is no transaction across them, so "roll it back" is not something a database
 * can be asked to do — it has to be done by running the opposite of each step that succeeded, in
 * reverse. Every step here publishes what it did, so the broker's own record of the run is
 * printed alongside the result: this is a saga made of real messages rather than of nothing.
 *
 * <p>Three runs, and the third is the one that matters. A compensation <em>itself</em> fails, the
 * saga does not throw, and what comes back names the step that could not be undone. That list is
 * the alert: everything else a saga reports is recoverable by construction, and this is not.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class OrderSaga {

    public record Order(String id) { }

    private static final String EXCHANGE = "saga.events";
    private static final String LEDGER = "saga.ledger";

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        try (AceMq mq = AceMq.connect(url)) {
            mq.declareExchange(EXCHANGE, "topic");
            mq.declareQueue(LEDGER);
            mq.bind(LEDGER, EXCHANGE, "#");

            List<String> ledger = new CopyOnWriteArrayList<>();
            try (MessageConsumer ignored = mq.consume(LEDGER, Order.class,
                    message -> ledger.add(message.routingKey().orElse("?")))) {

                // Every step and every compensation is a publish, so the ledger below is what
                // the broker actually saw rather than what this class says it did.
                Consumer<Order> reserveStock = announce(mq, "stock.reserved");
                Consumer<Order> releaseStock = announce(mq, "stock.released");
                Consumer<Order> takePayment = announce(mq, "payment.taken");
                Consumer<Order> refundPayment = announce(mq, "payment.refunded");
                Consumer<Order> bookCourier = announce(mq, "courier.booked");
                Consumer<Order> courierRefuses = order -> {
                    throw new IllegalStateException("no courier will collect from that postcode");
                };

                // Booking the courier has no compensation, and that is not an oversight: it is
                // the last step, so there is nothing after it that can fail and nothing of it to
                // undo. A step with no compensation is a claim, and the place to make it is at
                // the end.
                SagaResult everythingWorked = Saga.<Order>named("place-order")
                        .step("reserve-stock", reserveStock).compensateWith(releaseStock)
                        .step("take-payment", takePayment).compensateWith(refundPayment)
                        .step("book-courier", bookCourier)
                        .build()
                        .run(new Order("ORD-1"));

                waitFor(ledger, 3);
                System.out.printf("  run 1      complete=%s steps=%s%n",
                        everythingWorked.isComplete(), everythingWorked.completed());
                System.out.printf("             the broker saw: %s%n", ledger);
                require(everythingWorked.isComplete(), "the first run should have completed: " + everythingWorked);
                require(ledger.equals(List.of("stock.reserved", "payment.taken", "courier.booked")),
                        "the happy path should have published its three events in order, and published " + ledger);

                ledger.clear();
                SagaResult courierRefused = Saga.<Order>named("place-order")
                        .step("reserve-stock", reserveStock).compensateWith(releaseStock)
                        .step("take-payment", takePayment).compensateWith(refundPayment)
                        .step("book-courier", courierRefuses)
                        .build()
                        .run(new Order("ORD-2"));

                waitFor(ledger, 4);
                System.out.println();
                System.out.printf("  run 2      complete=%s compensated=%s failedAt=%s%n",
                        courierRefused.isComplete(), courierRefused.compensated(),
                        courierRefused.failedAt().orElse("-"));
                System.out.printf("             because %s%n",
                        courierRefused.failure().map(Throwable::getMessage).orElse("-"));
                System.out.printf("             the broker saw: %s%n", ledger);

                // Newest first. The later steps were built on the earlier ones, so undoing them
                // in the order they were done would release the stock while the payment that
                // paid for it was still standing.
                require(courierRefused.compensated() && !courierRefused.isComplete(),
                        "the second run should have compensated: " + courierRefused);
                require(ledger.equals(
                                List.of("stock.reserved", "payment.taken", "payment.refunded", "stock.released")),
                        "compensations run newest first, so the refund comes before the release, and the"
                                + " broker saw " + ledger);
                require(!courierRefused.hasUnresolved(),
                        "both compensations worked, so nothing should be unresolved: " + courierRefused);

                ledger.clear();
                Consumer<Order> warehouseRefuses = order -> {
                    throw new IllegalStateException("the warehouse will not release a picked reservation");
                };
                SagaResult halfUndone = Saga.<Order>named("place-order")
                        .step("reserve-stock", reserveStock).compensateWith(warehouseRefuses)
                        .step("take-payment", takePayment).compensateWith(refundPayment)
                        .step("book-courier", courierRefuses)
                        .build()
                        .run(new Order("ORD-3"));

                waitFor(ledger, 3);
                System.out.println();
                System.out.printf("  run 3      unresolved=%s — the row a person has to look at%n",
                        halfUndone.unresolved());
                System.out.printf("             the broker saw: %s%n", ledger);
                System.out.printf("             %s%n", halfUndone);

                // The compensation that failed did not stop the ones after it: the refund still
                // happened. Stopping would have left more undone than continuing did.
                require(halfUndone.unresolved().equals(List.of("reserve-stock")),
                        "the warehouse refusal should have left reserve-stock unresolved, and left "
                                + halfUndone.unresolved());
                require(ledger.equals(List.of("stock.reserved", "payment.taken", "payment.refunded")),
                        "the refund should still have run after the failed release, and the broker saw " + ledger);
            }
        }
    }

    /** A step, and the message that says it happened. */
    private static Consumer<Order> announce(AceMq mq, String event) {
        return order -> mq.publisher(EXCHANGE, event, Order.class).send(order);
    }

    /**
     * Waits for the broker to have delivered what the saga published.
     *
     * <p>A saga returns as soon as its last step does, and the events are still in flight at
     * that moment. Printing the ledger without waiting is how an example prints a different list
     * every third run.
     */
    private static void waitFor(List<String> ledger, int expected) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (ledger.size() < expected) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(
                        "expected " + expected + " events on the ledger, saw " + ledger);
            }
            Thread.sleep(20);
        }
        // A moment longer, so a fourth event that should not exist has a chance to show up and
        // fail the check below rather than arriving after it.
        Thread.sleep(250);
    }

    /** Fails the example rather than printing something untrue. */
    private static void require(boolean claim, String whatWasExpected) {
        if (!claim) {
            throw new IllegalStateException(whatWasExpected);
        }
    }
}
