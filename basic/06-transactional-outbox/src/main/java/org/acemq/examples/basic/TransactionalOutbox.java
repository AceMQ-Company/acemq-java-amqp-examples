package org.acemq.examples.basic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.OutboxRecord;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.patterns.JdbcOutboxStore;
import org.acemq.amqp.patterns.OutboxRelay;
import org.h2.jdbcx.JdbcDataSource;

/**
 * The dual-write problem, and the outbox that solves it.
 *
 * <p>Saving an order and publishing an event are two writes to two systems. Done in
 * sequence, a crash in between leaves one done and the other not: an order nobody was
 * told about, or a notification for an order that does not exist. No amount of retrying
 * fixes it, because the process that would do the retrying is the one that died.
 *
 * <p>The outbox makes it one write. The message is inserted in the same database
 * transaction as the business data, so it becomes durable exactly when the order does —
 * and a relay publishes it afterwards.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class TransactionalOutbox {

    public record OrderPlaced(String id, double total) { }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        JdbcDataSource database = new JdbcDataSource();
        database.setURL("jdbc:h2:mem:orders-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        database.setUser("sa");
        try (Connection setup = database.getConnection(); Statement statement = setup.createStatement()) {
            statement.execute("CREATE TABLE orders (id VARCHAR(64) PRIMARY KEY, total DECIMAL(10,2))");
        }

        try (AceMq mq = AceMq.connect(url)) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new");
            mq.bind("orders.new", "orders", "order.*");

            // The transaction the application is already using. The outbox writes into
            // this connection rather than opening its own, which is the whole trick:
            // there is no second commit to fail.
            try (Connection transaction = database.getConnection()) {
                transaction.setAutoCommit(false);

                JdbcOutboxStore outbox = new JdbcOutboxStore(() -> transaction, database);
                outbox.createSchemaIfAbsent();

                placeOrder(transaction, outbox, "o-1", 42.00, true);
                System.out.printf("  committed  order o-1 and its event, in one transaction%n");

                // And the case that makes the point: the same method, rolled back.
                placeOrder(transaction, outbox, "o-2", 99.00, false);
                System.out.printf("  rolled back order o-2 — neither the row nor the event survived%n");

                System.out.printf("  in the db  orders=%d, outbox rows pending=%d%n",
                        countOrders(database), outbox.pendingCount());
            }

            // Nothing has been published yet. The relay is what turns rows into
            // messages, and it runs separately — often in a different process.
            List<String> received = new CopyOnWriteArrayList<>();
            // Read as text, not as OrderPlaced. An outbox stores a payload that is
            // already serialised — that is what made it safe to write in the
            // transaction — and the relay republishes those bytes as they were. The
            // consumer therefore receives exactly the string that was stored, and
            // parses it with whatever the application already uses.
            try (MessageConsumer consumer = mq.consume("orders.new", String.class,
                    message -> received.add(message.envelope().id() + " " + message.payload()))) {

                JdbcOutboxStore relayStore = new JdbcOutboxStore(database::getConnection, database);
                try (OutboxRelay relay = new OutboxRelay(mq, relayStore, 10,
                        Duration.ofMillis(200), Duration.ofSeconds(30))) {
                    relay.start();
                    waitFor(() -> received.size() == 1, Duration.ofSeconds(30));
                }

                // A moment for a second message to appear if the rollback had leaked one.
                Thread.sleep(1_000);
                System.out.printf("  published  %s%n", received);
                System.out.printf("  outbox now pending=%d%n", relayStore.pendingCount());
            }
        }
    }

    /**
     * Saves an order and records the event it should publish, in one transaction.
     *
     * <p>In a Spring application this method carries {@code @Transactional} and the two
     * writes look exactly like this.
     */
    private static void placeOrder(Connection transaction, JdbcOutboxStore outbox,
            String id, double total, boolean commit) throws Exception {
        try (PreparedStatement insert = transaction.prepareStatement(
                "INSERT INTO orders (id, total) VALUES (?, ?)")) {
            insert.setString(1, id);
            insert.setDouble(2, total);
            insert.executeUpdate();
        }

        outbox.add(OutboxRecord.of("orders", "order.placed",
                Envelope.of("order.placed").id(id).build(),
                "{\"id\":\"" + id + "\",\"total\":" + total + "}"));

        if (commit) {
            transaction.commit();
        } else {
            // Whatever went wrong after the insert — a validation failure, a crash, a
            // downstream refusing — takes the message with it. There is no window in
            // which the order does not exist but the event does.
            transaction.rollback();
        }
    }

    private static int countOrders(JdbcDataSource database) throws Exception {
        try (Connection connection = database.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM orders")) {
            return rows.next() ? rows.getInt(1) : 0;
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier done, Duration limit) throws Exception {
        long deadline = System.nanoTime() + limit.toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("timed out waiting for the relay");
            }
            Thread.sleep(100);
        }
    }
}
