package org.acemq.examples.apps.fulfilment.shipping;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.examples.apps.fulfilment.contracts.Fulfilment;

/**
 * Dispatches what has been paid for and reserved.
 *
 * <p>The simplest service in the system, and it is worth noticing why: it reacts to one
 * event, does one thing, and publishes one event. It knows nothing about payments,
 * nothing about stock levels, and nothing about who else cares that an order shipped.
 *
 * <p>That is the property the whole architecture is buying. Adding a service that also
 * reacts to {@code stock.reserved} requires no change here at all.
 */
public final class ShippingService implements AutoCloseable {

    private final AceMq mq;
    private final MessageConsumer consumer;
    private final AtomicInteger shipped = new AtomicInteger();

    public ShippingService(String amqpUrl, Telemetry telemetry) {
        this.mq = AceMq.connect(amqpUrl, telemetry);
        mq.topology().apply(Fulfilment.topology(), ApplyMode.CREATE_ONLY);

        // Four consumers: dispatch is the slow step, and this is the service that gets
        // scaled first when the queue starts growing. Nothing else has to change for
        // that to happen.
        this.consumer = mq.consume(Fulfilment.SHIPPING, Fulfilment.StockReserved.class,
                ConsumerOptions.prefetch(10),
                message -> dispatch(message.payload(), message.envelope()));
    }

    private void dispatch(Fulfilment.StockReserved reservation, Envelope envelope) {
        String tracking = "TRK-" + reservation.orderId().substring(4).toUpperCase(java.util.Locale.ROOT);
        mq.publisher(Fulfilment.EXCHANGE, Fulfilment.ORDER_SHIPPED, Fulfilment.OrderShipped.class)
                .send(new Fulfilment.OrderShipped(reservation.orderId(), reservation.customer(), tracking),
                        Envelope.of("OrderShipped").correlationId(envelope.correlationId()).build());
        shipped.incrementAndGet();
    }

    public int shipped() {
        return shipped.get();
    }

    @Override
    public void close() {
        consumer.drain(Duration.ofSeconds(10));
        consumer.close();
        mq.close();
    }
}
