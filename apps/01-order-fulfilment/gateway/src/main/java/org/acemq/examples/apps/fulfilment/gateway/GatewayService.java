package org.acemq.examples.apps.fulfilment.gateway;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;

import javax.sql.DataSource;

import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.OutboxRecord;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.patterns.JdbcOutboxStore;
import org.acemq.amqp.patterns.OutboxRelay;
import org.acemq.examples.apps.fulfilment.contracts.Fulfilment;

/**
 * Where orders enter the system.
 *
 * <p>The edge of a system is where the dual-write problem lives: an order has to be saved
 * <em>and</em> announced, and doing those as two writes means a crash between them either
 * loses the announcement or announces something that was never saved. Neither is
 * recoverable by retrying, because the process that would retry is the one that died.
 *
 * <p>So the gateway does one write. The event is inserted in the same transaction as the
 * order, and a relay publishes it afterwards.
 */
public final class GatewayService implements AutoCloseable {

    private final AceMq mq;
    private final DataSource database;
    private final JdbcOutboxStore relayStore;
    private final OutboxRelay relay;

    public GatewayService(String amqpUrl, DataSource database, Telemetry telemetry) {
        this.database = database;
        this.mq = AceMq.connect(amqpUrl, telemetry);

        // Every service applies the whole topology. Applying it five times is safe and
        // means no service depends on another having started first -- there is no
        // deployment order to get wrong.
        mq.topology().apply(Fulfilment.topology(), ApplyMode.CREATE_ONLY);

        createSchema();

        // The relay uses its own connections, because it runs on its own schedule and
        // must not be inside anybody's request transaction.
        this.relayStore = new JdbcOutboxStore(this::freshConnection, database);
        this.relay = new OutboxRelay(mq, relayStore, 20, Duration.ofMillis(200), Duration.ofSeconds(30));
        relay.start();
    }

    /**
     * Takes an order.
     *
     * <p>In a real gateway this is the body of an HTTP handler and carries
     * {@code @Transactional}. The two writes look exactly like this.
     *
     * @return the id the customer is given
     */
    public String placeOrder(String customer, String sku, int quantity, double total) throws Exception {
        String orderId = "ord-" + UUID.randomUUID().toString().substring(0, 8);

        try (Connection transaction = database.getConnection()) {
            transaction.setAutoCommit(false);
            try {
                try (PreparedStatement insert = transaction.prepareStatement(
                        "INSERT INTO orders (id, customer, sku, quantity, total, status)"
                                + " VALUES (?, ?, ?, ?, ?, 'PLACED')")) {
                    insert.setString(1, orderId);
                    insert.setString(2, customer);
                    insert.setString(3, sku);
                    insert.setInt(4, quantity);
                    insert.setDouble(5, total);
                    insert.executeUpdate();
                }

                // The outbox writes through the caller's connection. That is the whole
                // trick: there is no second commit that can fail on its own.
                JdbcOutboxStore outbox = new JdbcOutboxStore(() -> transaction, database);
                outbox.add(OutboxRecord.of(Fulfilment.EXCHANGE, Fulfilment.ORDER_PLACED,
                        Envelope.of("OrderPlaced").id(orderId).correlationId(orderId).build(),
                        json(orderId, customer, sku, quantity, total)));

                transaction.commit();
            } catch (Exception e) {
                transaction.rollback();
                throw e;
            }
        }
        return orderId;
    }

    /** What the gateway believes about an order, which is only what it was told. */
    public String statusOf(String orderId) throws Exception {
        try (Connection connection = database.getConnection();
                PreparedStatement query = connection.prepareStatement(
                        "SELECT status FROM orders WHERE id = ?")) {
            query.setString(1, orderId);
            try (var rows = query.executeQuery()) {
                return rows.next() ? rows.getString(1) : "UNKNOWN";
            }
        }
    }

    public long pendingInOutbox() {
        return relayStore.pendingCount();
    }

    private Connection freshConnection() {
        try {
            return database.getConnection();
        } catch (Exception e) {
            throw new IllegalStateException("could not open a connection for the relay", e);
        }
    }

    private void createSchema() {
        try (Connection setup = database.getConnection(); Statement statement = setup.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS orders ("
                    + "id VARCHAR(64) PRIMARY KEY, customer VARCHAR(128), sku VARCHAR(64),"
                    + " quantity INT, total DECIMAL(10,2), status VARCHAR(32))");
            new JdbcOutboxStore(this::freshConnection, database).createSchemaIfAbsent();
        } catch (Exception e) {
            throw new IllegalStateException("could not create the gateway schema", e);
        }
    }

    /**
     * The payload, serialised here rather than by the publisher.
     *
     * <p>An outbox stores bytes, not objects — that is what made it safe to write inside
     * the transaction. The relay republishes exactly these bytes, so this is the wire
     * format and it has to match what the consumers expect.
     */
    private static String json(String orderId, String customer, String sku, int quantity, double total) {
        return "{\"orderId\":\"" + orderId + "\",\"customer\":\"" + customer + "\",\"sku\":\"" + sku
                + "\",\"quantity\":" + quantity + ",\"total\":" + total + "}";
    }

    @Override
    public void close() {
        relay.close();
        mq.close();
    }
}
