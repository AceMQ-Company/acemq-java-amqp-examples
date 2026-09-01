package org.acemq.examples.apps.fulfilment.payments;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.Codecs;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.patterns.JdbcIdempotencyStore;
import org.acemq.examples.apps.fulfilment.contracts.Fulfilment;

/**
 * Takes the money.
 *
 * <p>This is the service where at-least-once delivery stops being a technicality. Every
 * other service in this system can handle a message twice and produce the same outcome;
 * this one cannot, because the second charge is real money belonging to a real customer.
 *
 * <p>So it claims each order in a shared store before charging, and confirms afterwards.
 * The store is shared rather than in-memory because there is more than one instance of
 * this service in production, and an in-memory store makes each instance individually
 * idempotent while the fleet is not.
 */
public final class PaymentsService implements AutoCloseable {

    /** Over this, a human has to look at it. Every payment system has one of these. */
    private static final double AUTOMATIC_LIMIT = 1_000.00;

    private final AceMq mq;
    private final MessageConsumer consumer;
    private final JdbcIdempotencyStore charged;
    private final AtomicInteger captures = new AtomicInteger();
    private final AtomicInteger declines = new AtomicInteger();
    private final AtomicInteger duplicatesRefused = new AtomicInteger();

    public PaymentsService(String amqpUrl, DataSource database, Telemetry telemetry) {
        this.mq = AceMq.connect(amqpUrl, telemetry);
        mq.topology().apply(Fulfilment.topology(), ApplyMode.CREATE_ONLY);

        // A generous claim timeout: it has to outlast the slowest charge, because a
        // claim that expires while the payment gateway is still thinking is a claim
        // another instance will take, and then the customer pays twice.
        this.charged = new JdbcIdempotencyStore(
                database, Duration.ofDays(7), Duration.ofMinutes(2), "payments_handled");
        charged.createSchemaIfAbsent();

        // Explicit JSON: these messages come from the gateway's outbox, which stores
        // already-serialised bytes and republishes them as they were.
        ConsumerOptions options = ConsumerOptions.prefetch(20)
                .as(Codecs.byName("json"))
                .withRetry(RetryPolicy.exponential(4, Duration.ofMillis(200), Duration.ofSeconds(5)));

        this.consumer = mq.consume(Fulfilment.PAYMENTS, Fulfilment.OrderPlaced.class, options,
                message -> charge(message.payload(), message.envelope()));
    }

    private void charge(Fulfilment.OrderPlaced order, Envelope envelope) {
        // The claim is the whole safety net. A redelivery -- from a broker restart, a
        // consumer that died mid-handle, or a relay that published twice -- loses here.
        if (!charged.claim(order.orderId())) {
            duplicatesRefused.incrementAndGet();
            return;
        }

        if (order.total() > AUTOMATIC_LIMIT) {
            mq.publisher(Fulfilment.EXCHANGE, Fulfilment.PAYMENT_DECLINED, Fulfilment.PaymentDeclined.class)
                    .send(new Fulfilment.PaymentDeclined(order.orderId(), order.customer(),
                                    "over the automatic limit"),
                            // The correlation id is what makes five services one story in a
                            // log aggregator. Carrying it forward is not optional.
                            Envelope.of("PaymentDeclined").correlationId(envelope.correlationId()).build());
            declines.incrementAndGet();
            charged.confirm(order.orderId());
            return;
        }

        mq.publisher(Fulfilment.EXCHANGE, Fulfilment.PAYMENT_CAPTURED, Fulfilment.PaymentCaptured.class)
                .send(new Fulfilment.PaymentCaptured(order.orderId(), order.customer(),
                                order.sku(), order.quantity(), order.total()),
                        Envelope.of("PaymentCaptured").correlationId(envelope.correlationId()).build());
        captures.incrementAndGet();

        // Confirmed only after the outcome is published. Confirming first would mean a
        // crash in between leaves the order marked as charged with nothing downstream
        // ever told -- an order that took the money and stopped.
        charged.confirm(order.orderId());
    }

    public int captured() {
        return captures.get();
    }

    public int declined() {
        return declines.get();
    }

    /** How many redeliveries were recognised and refused. Worth graphing. */
    public int duplicatesRefused() {
        return duplicatesRefused.get();
    }

    @Override
    public void close() {
        consumer.drain(Duration.ofSeconds(10));
        consumer.close();
        mq.close();
    }
}
