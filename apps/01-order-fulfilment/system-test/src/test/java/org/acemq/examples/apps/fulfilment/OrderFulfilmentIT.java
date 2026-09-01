package org.acemq.examples.apps.fulfilment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;

import org.acemq.amqp.api.Telemetry;
import org.acemq.examples.apps.fulfilment.gateway.GatewayService;
import org.acemq.examples.apps.fulfilment.inventory.InventoryService;
import org.acemq.examples.apps.fulfilment.notifications.NotificationsService;
import org.acemq.examples.apps.fulfilment.payments.PaymentsService;
import org.acemq.examples.apps.fulfilment.shipping.ShippingService;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * The whole system, five services, one broker.
 *
 * <p>In production these are five deployments. Here they run in one JVM against one real
 * RabbitMQ, which exercises every queue, every hop and every failure path for the cost of
 * a single container — and would fail if any service stopped agreeing with the contracts.
 */
@Testcontainers
@DisplayName("apps/01 — order fulfilment")
class OrderFulfilmentIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    private GatewayService gateway;
    private PaymentsService payments;
    private InventoryService inventory;
    private ShippingService shipping;
    private NotificationsService notifications;

    @BeforeEach
    void startTheSystem() {
        String url = BROKER.getAmqpUrl();

        // A database per service, because services do not share one. The moment two
        // services read the same table, the deployment boundary is a fiction.
        gateway = new GatewayService(url, database("gateway"), Telemetry.NONE);
        payments = new PaymentsService(url, database("payments"), Telemetry.NONE);
        inventory = new InventoryService(url, Telemetry.NONE).withStock("WIDGET", 10);
        shipping = new ShippingService(url, Telemetry.NONE);
        notifications = new NotificationsService(url, Telemetry.NONE);
    }

    @AfterEach
    void stopTheSystem() {
        for (AutoCloseable service : new AutoCloseable[] {
                notifications, shipping, inventory, payments, gateway}) {
            try {
                if (service != null) {
                    service.close();
                }
            } catch (Exception ignored) {
                // Shutting down is not the subject of this test.
            }
        }
    }

    @Test
    @Timeout(180)
    void anOrderTravelsThroughEveryService() throws Exception {
        String orderId = gateway.placeOrder("ada", "WIDGET", 2, 42.00);

        waitFor(() -> shipping.shipped() == 1);

        // One order in at the gateway, and every service downstream acted exactly once.
        assertThat(payments.captured()).isEqualTo(1);
        assertThat(inventory.reserved()).isEqualTo(1);
        assertThat(shipping.shipped()).isEqualTo(1);

        // Stock actually moved. Without this the reservation is a log line.
        assertThat(inventory.stockOf("WIDGET")).isEqualTo(8);

        // And the customer's view is the whole story, assembled from events published by
        // four different services that never spoke to each other.
        // Four events, and the wait is for the final count rather than an intermediate
        // one: polling for "== 3" can miss the moment the third arrives and the fourth
        // follows, which is a flake that only shows up on a fast machine.
        waitFor(() -> notifications.timelineOf(orderId).size() >= 4);
        assertThat(notifications.timelineOf(orderId))
                .as("the timeline is built from the correlation id alone")
                .containsExactly("OrderPlaced", "PaymentCaptured", "StockReserved", "OrderShipped");

        // The outbox is empty, so nothing is waiting to be published.
        assertThat(gateway.pendingInOutbox()).isZero();
    }

    @Test
    @Timeout(180)
    void aFlakyWarehouseIsRetriedRatherThanFailed() throws Exception {
        inventory.withFlakyWarehouse(2);

        String orderId = gateway.placeOrder("grace", "WIDGET", 1, 10.00);

        waitFor(() -> shipping.shipped() == 1);

        // Two failures, then success. The order was never lost and no human was involved.
        assertThat(inventory.retried()).isGreaterThanOrEqualTo(2);
        assertThat(inventory.reserved()).isEqualTo(1);
        assertThat(notifications.timelineOf(orderId)).contains("OrderShipped");
    }

    @Test
    @Timeout(180)
    void anOrderOverTheLimitStopsAtPayments() throws Exception {
        String orderId = gateway.placeOrder("charles", "WIDGET", 1, 5_000.00);

        waitFor(() -> payments.declined() == 1);

        // Nothing downstream ran, which is the point of declining before reserving:
        // stock held for an order that cannot be paid for is stock nobody releases.
        assertThat(inventory.reserved()).isZero();
        assertThat(shipping.shipped()).isZero();
        assertThat(inventory.stockOf("WIDGET")).isEqualTo(10);

        waitFor(() -> notifications.timelineOf(orderId).size() == 2);
        assertThat(notifications.timelineOf(orderId))
                .containsExactly("OrderPlaced", "PaymentDeclined");
    }

    @Test
    @Timeout(180)
    void thereIsNotEnoughStockAndRetryingWouldNotHelp() throws Exception {
        String orderId = gateway.placeOrder("alan", "WIDGET", 99, 99.00);

        waitFor(() -> inventory.rejected() == 1);

        // The money was taken and the stock was not there. In a real system this is
        // where a refund is triggered -- the customer is told, and the compensation is
        // somebody's job. It is deliberately visible rather than swallowed.
        assertThat(payments.captured()).isEqualTo(1);
        assertThat(shipping.shipped()).isZero();

        waitFor(() -> notifications.timelineOf(orderId).size() == 3);
        assertThat(notifications.timelineOf(orderId))
                .containsExactly("OrderPlaced", "PaymentCaptured", "StockUnavailable");
    }

    /** A fresh in-memory database per service, per test. */
    private static JdbcDataSource database(String service) {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + service + "-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        source.setUser("sa");
        return source;
    }

    private static void waitFor(java.util.function.BooleanSupplier done) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the system did not reach the expected state in time");
            }
            Thread.sleep(50);
        }
    }
}
