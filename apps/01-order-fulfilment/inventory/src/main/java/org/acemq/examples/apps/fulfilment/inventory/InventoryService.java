package org.acemq.examples.apps.fulfilment.inventory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.AceFatalException;
import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.examples.apps.fulfilment.contracts.Fulfilment;

/**
 * Holds stock for orders that have been paid for.
 *
 * <p>The service that talks to something unreliable. A warehouse system that times out is
 * the ordinary case, not the exception, and the two failures have to be told apart:
 *
 * <ul>
 *   <li>the warehouse did not answer — retry, it will probably work in a moment;
 *   <li>there are three left and the order wants ten — retrying changes nothing.
 * </ul>
 *
 * <p>The first is a plain exception and goes up the retry ladder. The second is an
 * {@link AceFatalException}, which says "do not retry this" and sends it straight to the
 * dead-letter queue where a person can see it.
 */
public final class InventoryService implements AutoCloseable {

    private final AceMq mq;
    private final MessageConsumer consumer;
    private final Map<String, Integer> stock = new ConcurrentHashMap<>();
    private final AtomicInteger reserved = new AtomicInteger();
    private final AtomicInteger rejected = new AtomicInteger();
    private final AtomicInteger warehouseCalls = new AtomicInteger();

    /** How many warehouse calls fail before it starts working. Set by the tests. */
    private volatile int failuresToSimulate;

    public InventoryService(String amqpUrl, Telemetry telemetry) {
        this.mq = AceMq.connect(amqpUrl, telemetry);
        mq.topology().apply(Fulfilment.topology(), ApplyMode.CREATE_ONLY);

        // The ladder waits inside the broker rather than on this thread: a failed
        // message sits in a delay queue and comes back, instead of occupying a consumer
        // that could be handling the orders behind it.
        ConsumerOptions options = ConsumerOptions.prefetch(20)
                .withRetry(RetryPolicy.exponential(4, Duration.ofMillis(200), Duration.ofSeconds(5)));

        this.consumer = mq.consume(Fulfilment.INVENTORY, Fulfilment.PaymentCaptured.class, options,
                message -> reserve(message.payload(), message.envelope()));
    }

    public InventoryService withStock(String sku, int quantity) {
        stock.put(sku, quantity);
        return this;
    }

    /** Makes the next {@code count} warehouse calls fail, the way a real one does. */
    public InventoryService withFlakyWarehouse(int count) {
        this.failuresToSimulate = count;
        return this;
    }

    private void reserve(Fulfilment.PaymentCaptured payment, Envelope envelope) {
        // The transient failure. Nothing is wrong with the message, so it goes back on
        // the ladder and arrives again shortly.
        if (warehouseCalls.incrementAndGet() <= failuresToSimulate) {
            throw new IllegalStateException("warehouse did not respond");
        }

        Integer available = stock.get(payment.sku());
        if (available == null || available < payment.quantity()) {
            // The permanent one. Retrying will not conjure stock, and four more
            // attempts only delay telling the customer.
            mq.publisher(Fulfilment.EXCHANGE, Fulfilment.STOCK_UNAVAILABLE,
                            Fulfilment.StockUnavailable.class)
                    .send(new Fulfilment.StockUnavailable(payment.orderId(), payment.customer(),
                                    payment.sku(), "only " + (available == null ? 0 : available) + " left"),
                            Envelope.of("StockUnavailable").correlationId(envelope.correlationId()).build());
            rejected.incrementAndGet();
            return;
        }

        stock.merge(payment.sku(), -payment.quantity(), Integer::sum);
        mq.publisher(Fulfilment.EXCHANGE, Fulfilment.STOCK_RESERVED, Fulfilment.StockReserved.class)
                .send(new Fulfilment.StockReserved(payment.orderId(), payment.customer(),
                                payment.sku(), payment.quantity()),
                        Envelope.of("StockReserved").correlationId(envelope.correlationId()).build());
        reserved.incrementAndGet();
    }

    public int reserved() {
        return reserved.get();
    }

    public int rejected() {
        return rejected.get();
    }

    public int stockOf(String sku) {
        return stock.getOrDefault(sku, 0);
    }

    public long retried() {
        return consumer.retried();
    }

    @Override
    public void close() {
        consumer.drain(Duration.ofSeconds(10));
        consumer.close();
        mq.close();
    }
}
