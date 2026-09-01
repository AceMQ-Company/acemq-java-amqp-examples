package org.acemq.examples.apps.fulfilment.contracts;

import java.util.Map;

import org.acemq.amqp.api.Topology;

/**
 * What every service in this system agrees on, and nothing else.
 *
 * <p>The events, the exchange, the queue each service reads, and the routing keys that
 * connect them. In a larger estate this is what a schema registry holds; here it is a jar,
 * versioned and released like any other dependency.
 *
 * <p>What is deliberately <em>not</em> here: any service's domain model, any database
 * access, any shared "helper". A contracts module that grows those stops being a contract
 * and becomes a shared library, which is how five services turn back into one deployable
 * that happens to have five main methods.
 */
public final class Fulfilment {

    private Fulfilment() { }

    /** One topic exchange. Every event in the system is published here. */
    public static final String EXCHANGE = "fulfilment";

    // ---- events -------------------------------------------------------------------
    //
    // Records, so they serialise to JSON without ceremony and cannot be half-built.
    // Each carries the order id, because that is the only identifier every service
    // shares -- and correlation across five services is otherwise guesswork.

    /** Someone placed an order. Published by the gateway, from its outbox. */
    public record OrderPlaced(String orderId, String customer, String sku, int quantity, double total) { }

    /** The money is ours. Published by payments. */
    public record PaymentCaptured(String orderId, String customer, String sku, int quantity, double amount) { }

    /** It is not, and will not be. Published by payments; nothing downstream proceeds. */
    public record PaymentDeclined(String orderId, String customer, String reason) { }

    /** Stock is held for this order. Published by inventory. */
    public record StockReserved(String orderId, String customer, String sku, int quantity) { }

    /** There is not enough. Published by inventory; the money must be given back. */
    public record StockUnavailable(String orderId, String customer, String sku, String reason) { }

    /** On its way. Published by shipping. */
    public record OrderShipped(String orderId, String customer, String tracking) { }

    // ---- routing keys -------------------------------------------------------------
    //
    // "fulfilment.<aggregate>.<past-tense-verb>". The aggregate in the middle is what
    // lets a service subscribe to everything about orders without naming each event,
    // and lets notifications subscribe to everything at all.

    public static final String ORDER_PLACED = "fulfilment.order.placed";
    public static final String PAYMENT_CAPTURED = "fulfilment.payment.captured";
    public static final String PAYMENT_DECLINED = "fulfilment.payment.declined";
    public static final String STOCK_RESERVED = "fulfilment.stock.reserved";
    public static final String STOCK_UNAVAILABLE = "fulfilment.stock.unavailable";
    public static final String ORDER_SHIPPED = "fulfilment.order.shipped";

    // ---- queues -------------------------------------------------------------------
    //
    // A queue per service, named after the service rather than after the event. That
    // is what makes them independent: two services wanting the same event each get
    // their own copy, and neither can starve the other.

    public static final String PAYMENTS = "fulfilment.payments";
    public static final String INVENTORY = "fulfilment.inventory";
    public static final String SHIPPING = "fulfilment.shipping";
    public static final String NOTIFICATIONS = "fulfilment.notifications";

    /**
     * The whole system's topology, as one value.
     *
     * <p>Every service applies this on start-up. Applying the same topology from five
     * places is safe and is the point: no service depends on another having started
     * first, and there is no deployment order to get wrong. The alternative — each
     * service declaring only its own queue — means the first service to start finds
     * nothing to publish into.
     *
     * @return what must exist for this system to work
     */
    public static Topology topology() {
        return Topology.define()
                .exchange(EXCHANGE, "topic")

                // Payments acts on new orders.
                .classicQueue(PAYMENTS, Map.of())
                .bind(PAYMENTS, EXCHANGE, ORDER_PLACED)

                // Inventory acts once the money is taken, not before. Reserving stock
                // for an order that cannot be paid for is how a warehouse fills with
                // holds nobody releases.
                .classicQueue(INVENTORY, Map.of())
                .bind(INVENTORY, EXCHANGE, PAYMENT_CAPTURED)

                // Shipping needs stock held.
                .classicQueue(SHIPPING, Map.of())
                .bind(SHIPPING, EXCHANGE, STOCK_RESERVED)

                // Notifications wants everything, which is what a wildcard is for.
                .classicQueue(NOTIFICATIONS, Map.of())
                .bind(NOTIFICATIONS, EXCHANGE, "fulfilment.#")

                .build();
    }
}
