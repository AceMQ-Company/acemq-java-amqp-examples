package org.acemq.examples.apps.policy.policies;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.OutboxRecord;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.core.Responder;
import org.acemq.amqp.patterns.JdbcOutboxStore;
import org.acemq.amqp.patterns.OutboxRelay;
import org.acemq.examples.apps.policy.contracts.Policies;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Applications and policies: the module that owns the records everything else refers to.
 *
 * <p>Two things here are worth the reading.
 *
 * <p><strong>The outbox is still necessary.</strong> This is a monolith with one database, so
 * the usual argument for an outbox — two services, two datastores — does not apply. It applies
 * anyway, because the two systems that must agree are *this database* and *the broker*, and no
 * transaction spans both. Saving an application and announcing it are still two writes, and a
 * crash between them still loses one. A monolith removes the distributed transaction between
 * modules; it does not remove the one between a module and its broker.
 *
 * <p><strong>Claims asks this module a question rather than reading its tables.</strong> The
 * module exposes a {@link Responder} on a queue. In one JVM a direct call would obviously work,
 * which is exactly why the discipline matters: the moment claims calls a method here, the two
 * modules are one module, and no amount of package structure will separate them again.
 */
public final class PolicyModule implements AutoCloseable {

    private final AceMq mq;
    private final DataSource dataSource;
    private final Connection transactional;
    private final JdbcOutboxStore outbox;
    private final OutboxRelay relay;
    private final MessageConsumer accepted;
    private final Responder lookups;
    private final ObjectMapper json = new ObjectMapper();
    private final AtomicInteger issued = new AtomicInteger();

    public PolicyModule(AceMq mq, DataSource dataSource) throws SQLException {
        this.mq = mq;
        this.dataSource = dataSource;
        this.transactional = dataSource.getConnection();
        this.transactional.setAutoCommit(false);
        createSchema();

        // The write side takes the transaction's own connection; the relay gets its own from
        // the pool. A record written on a different connection is a record in a different
        // transaction, which is the bug the outbox exists to prevent.
        this.outbox = new JdbcOutboxStore(() -> transactional, dataSource);
        this.outbox.createSchemaIfAbsent();

        this.relay = new OutboxRelay(mq, outbox);
        this.relay.start();

        this.accepted = mq.consume(
                Policies.POLICIES, Policies.ApplicationAccepted.class, message -> issue(message.payload()));

        this.lookups = mq.respond(
                Policies.POLICY_LOOKUP, Policies.PolicyQuery.class, this::statusOf);
    }

    /**
     * Takes an application, and announces it, in one transaction.
     *
     * @param applicant who is applying
     * @param product what they want
     * @param sumAssured how much cover
     * @param age the applicant's age, which underwriting will price on
     * @return the application id
     */
    public String submit(String applicant, String product, int sumAssured, int age) {
        String applicationId = "APP-" + UUID.randomUUID().toString().substring(0, 8);
        try {
            try (PreparedStatement insert = transactional.prepareStatement(
                    "INSERT INTO applications (id, applicant, product, sum_assured, age) VALUES (?, ?, ?, ?, ?)")) {
                insert.setString(1, applicationId);
                insert.setString(2, applicant);
                insert.setString(3, product);
                insert.setInt(4, sumAssured);
                insert.setInt(5, age);
                insert.executeUpdate();
            }

            outbox.add(OutboxRecord.of(
                    Policies.EXCHANGE,
                    Policies.APPLICATION_SUBMITTED,
                    Envelope.of("ApplicationSubmitted").correlationId(applicationId).build(),
                    json.writeValueAsString(new Policies.ApplicationSubmitted(
                            applicationId, applicant, product, sumAssured, age))));

            // One commit decides both. Either the application exists and the event is queued,
            // or neither happened.
            transactional.commit();
            return applicationId;
        } catch (Exception e) {
            rollbackQuietly();
            throw new IllegalStateException("could not submit the application for " + applicant, e);
        }
    }

    /** Underwriting said yes, so the policy exists. */
    private void issue(Policies.ApplicationAccepted accepted) {
        String policyId = "POL-" + UUID.randomUUID().toString().substring(0, 8);
        try {
            try (PreparedStatement insert = transactional.prepareStatement(
                    "INSERT INTO policies (id, application_id, applicant, product, premium) VALUES (?, ?, ?, ?, ?)")) {
                insert.setString(1, policyId);
                insert.setString(2, accepted.applicationId());
                insert.setString(3, accepted.applicant());
                insert.setString(4, accepted.product());
                insert.setInt(5, accepted.annualPremium());
                insert.executeUpdate();
            }

            outbox.add(OutboxRecord.of(
                    Policies.EXCHANGE,
                    Policies.POLICY_ISSUED,
                    Envelope.of("PolicyIssued").correlationId(accepted.applicationId()).build(),
                    json.writeValueAsString(new Policies.PolicyIssued(
                            policyId,
                            accepted.applicationId(),
                            accepted.applicant(),
                            accepted.product(),
                            accepted.annualPremium()))));

            transactional.commit();
            issued.incrementAndGet();
        } catch (Exception e) {
            rollbackQuietly();
            throw new IllegalStateException("could not issue a policy for " + accepted.applicationId(), e);
        }
    }

    /** Answers the question claims asks, without claims touching this module's tables. */
    private Policies.PolicyStatus statusOf(Policies.PolicyQuery query) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(
                        "SELECT premium FROM policies WHERE id = ?")) {
            select.setString(1, query.policyId());
            try (ResultSet rows = select.executeQuery()) {
                return rows.next()
                        ? new Policies.PolicyStatus(query.policyId(), true, rows.getInt(1))
                        : new Policies.PolicyStatus(query.policyId(), false, 0);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("could not look up " + query.policyId(), e);
        }
    }

    /** @return how many policies have been issued */
    public int issued() {
        return issued.get();
    }

    /** @return the premium recorded for a policy, when it exists */
    public Optional<Integer> premiumOf(String policyId) {
        Policies.PolicyStatus status = statusOf(new Policies.PolicyQuery(policyId));
        return status.inForce() ? Optional.of(status.annualPremium()) : Optional.empty();
    }

    private void createSchema() throws SQLException {
        try (Statement statement = transactional.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS applications ("
                    + "id VARCHAR(64) PRIMARY KEY, applicant VARCHAR(255), product VARCHAR(64),"
                    + " sum_assured INT, age INT)");
            statement.execute("CREATE TABLE IF NOT EXISTS policies ("
                    + "id VARCHAR(64) PRIMARY KEY, application_id VARCHAR(64), applicant VARCHAR(255),"
                    + " product VARCHAR(64), premium INT)");
        }
        transactional.commit();
    }

    private void rollbackQuietly() {
        try {
            transactional.rollback();
        } catch (SQLException ignored) {
            // Already failing; the original cause is the one worth reporting.
        }
    }

    @Override
    public void close() throws Exception {
        lookups.close();
        accepted.close();
        relay.close();
        transactional.close();
    }
}
