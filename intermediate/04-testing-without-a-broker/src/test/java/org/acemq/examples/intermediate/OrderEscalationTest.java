package org.acemq.examples.intermediate;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.test.InMemoryTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What a test of messaging code looks like when it does not need a broker.
 *
 * <p>This is a plain unit test — surefire, no {@code @Testcontainers}, no {@code IT}
 * suffix. It runs on every save. The integration tests in this repository exist to prove
 * the library works against RabbitMQ; tests like this one exist to prove *your* rules are
 * right, and there is no reason for those to wait on Docker.
 */
@DisplayName("order escalation")
class OrderEscalationTest {

    @AfterEach
    void discardTheBroker() {
        // Between tests, not before. A test that leaves state behind should fail the
        // next one loudly rather than being cleaned up on the way in, where the failure
        // would depend on execution order.
        InMemoryTransport.reset();
    }

    @Test
    void an_order_over_the_limit_is_escalated() {
        assertThat(escalationsFor(new TestingWithoutABroker.Order("large", 5_000.00)))
                .containsExactly("large");
    }

    @Test
    void an_order_under_it_is_not() {
        // The test that proves the other one is not passing by accident.
        assertThat(escalationsFor(new TestingWithoutABroker.Order("small", 10.00)))
                .isEmpty();
    }

    /** Runs one order through the rule and returns what was escalated. */
    private List<String> escalationsFor(TestingWithoutABroker.Order order) {
        List<String> escalated = new CopyOnWriteArrayList<>();
        // Counted separately from the escalations, so the wait below is for the message
        // having been *handled* rather than for it having been escalated -- otherwise the
        // "not escalated" case has nothing to wait for and passes before the handler runs.
        AtomicInteger handled = new AtomicInteger();

        try (AceMq mq = AceMq.connect("memory://orders")) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.large", QueueType.CLASSIC, java.util.Map.of());
            mq.bind("orders.large", "orders", "order.placed");

            try (MessageConsumer consumer = mq.consume("orders.large",
                    TestingWithoutABroker.Order.class, message -> {
                        if (message.payload().total() > 1_000) {
                            escalated.add(message.payload().id());
                        }
                        handled.incrementAndGet();
                    })) {

                mq.publisher("orders", "order.placed", TestingWithoutABroker.Order.class).send(order);
                waitFor(() -> handled.get() == 1);
            }
        }
        return escalated;
    }

    private static void waitFor(java.util.function.BooleanSupplier done) {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the message was never handled");
            }
            Thread.onSpinWait();
        }
    }
}
