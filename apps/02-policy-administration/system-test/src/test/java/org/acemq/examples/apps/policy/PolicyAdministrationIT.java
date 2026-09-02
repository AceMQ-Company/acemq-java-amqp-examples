package org.acemq.examples.apps.policy;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.examples.apps.policy.billing.BillingModule;
import org.acemq.examples.apps.policy.claims.ClaimsModule;
import org.acemq.examples.apps.policy.contracts.Policies;
import org.acemq.examples.apps.policy.documents.DocumentModule;
import org.acemq.examples.apps.policy.policies.PolicyModule;
import org.acemq.examples.apps.policy.underwriting.UnderwritingModule;
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
 * The whole application, in one JVM, against a real broker.
 *
 * <p>Which is what it is in production too — that is the point of a monolith. The difference
 * from apps/01 is not the number of processes but where the boundaries are: here they are Maven
 * modules and a broker, and this test is the only place that depends on more than one of them.
 */
@Testcontainers
@DisplayName("policy administration")
class PolicyAdministrationIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    private AceMq mq;
    private PolicyModule policies;
    private UnderwritingModule underwriting;
    private DocumentModule documents;
    private BillingModule billing;
    private ClaimsModule claims;

    @BeforeEach
    void startTheApplication() throws Exception {
        // One connection for the whole application, because it is one application. In apps/01
        // each service had its own; here sharing one is correct and is the only thing that
        // genuinely differs at the transport level.
        mq = AceMq.connect(BROKER.getAmqpUrl(), Telemetry.NONE);
        mq.topology().apply(Policies.topology(), ApplyMode.CREATE_ONLY);

        // One database, several modules — the monolith's actual advantage. The outbox still
        // has to exist, because the broker is not in this database's transaction.
        JdbcDataSource database = database("policy");

        policies = new PolicyModule(mq, database);
        underwriting = new UnderwritingModule(mq);
        documents = new DocumentModule(mq);
        billing = new BillingModule(mq, database);
        claims = new ClaimsModule(mq);
    }

    @AfterEach
    void stopTheApplication() {
        for (AutoCloseable module : new AutoCloseable[] {claims, billing, underwriting, policies}) {
            try {
                if (module != null) {
                    module.close();
                }
            } catch (Exception ignored) {
                // Shutting down is not the subject of this test.
            }
        }
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("an ordinary application becomes a policy, and the premium is taken once")
    void theHappyPath() {
        policies.submit("A. Applicant", "TERM-LIFE", 100_000, 40);

        // Submitted -> underwritten -> issued -> charged, with no module calling another.
        waitFor(() -> billing.charges().size() == 1);

        assertThat(underwriting.accepted()).isEqualTo(1);
        assertThat(policies.issued()).isEqualTo(1);
        assertThat(billing.charges()).hasSize(1);

        // 100 base + 20 age loading, from the rating table in the pricing stage.
        String policyId = billing.charges().get(0);
        assertThat(policies.premiumOf(policyId)).contains(120);
    }

    @Test
    @Timeout(180)
    @DisplayName("an application above the automatic limit is referred, and never becomes a policy")
    void referredAboveTheLimit() {
        policies.submit("B. Applicant", "TERM-LIFE", 750_000, 35);

        waitFor(() -> underwriting.declined() == 1);

        // Nothing downstream ran. Billing charging for a referred application would be the
        // expensive version of this bug, which is why it is bound to PolicyIssued and not to
        // the application.
        assertThat(policies.issued()).isZero();
        assertThat(billing.charges()).isEmpty();
    }

    @Test
    @Timeout(180)
    @DisplayName("a claim is assessed against an answer from policies, not against a local copy")
    void claimsAskRatherThanRead() {
        policies.submit("C. Applicant", "TERM-LIFE", 50_000, 30);
        waitFor(() -> billing.charges().size() == 1);
        String policyId = billing.charges().get(0);

        claims.submit(policyId, 5_000, "windscreen");
        // A policy nobody issued. The lookup is what makes this answerable at all.
        claims.submit("POL-does-not-exist", 5_000, "windscreen");

        waitFor(() -> claims.settled() == 1 && claims.rejected() == 1);
    }

    @Test
    @Timeout(180)
    @DisplayName("a large document travels as a claim check, not as a message")
    void documentsTravelByReference() {
        policies.submit("D. Applicant", "TERM-LIFE", 60_000, 45);
        waitFor(() -> billing.charges().size() == 1);
        String policyId = billing.charges().get(0);

        // Four megabytes, which is a small scan and a large message.
        byte[] scan = new byte[4 * 1024 * 1024];
        String key = documents.store(policyId, "medical-report", scan);

        assertThat(documents.fetch(key)).isPresent();
        assertThat(documents.fetch(key).get()).hasSize(scan.length);

        // What crossed the broker is the key and the size. The whole event is a few hundred
        // bytes; the four megabytes never went near a queue.
        assertThat(key).contains(policyId).contains("medical-report");
    }

    @Test
    @Timeout(180)
    @DisplayName("three copies of one event charge once; a genuinely different event still charges")
    void billingIsIdempotent() {
        policies.submit("E. Applicant", "TERM-LIFE", 80_000, 50);
        waitFor(() -> billing.charges().size() == 1);
        String policyId = billing.charges().get(0);
        int premium = policies.premiumOf(policyId).orElseThrow();

        // Three copies carrying one message id: what a redelivery looks like from the
        // consumer's side, and what the idempotency store exists to absorb.
        String messageId = "redelivery-" + UUID.randomUUID();
        for (int i = 0; i < 3; i++) {
            mq.publisher(Policies.EXCHANGE, Policies.POLICY_ISSUED, Policies.PolicyIssued.class)
                    .send(new Policies.PolicyIssued(policyId, "APP-x", "E. Applicant", "TERM-LIFE", premium),
                            org.acemq.amqp.api.Envelope.of("PolicyIssued").id(messageId).build());
        }

        // Two, not one -- and the difference is the whole point. The store deduplicates by
        // message id, so the three copies are one charge. They are not the *same* message as
        // the original issue, which had an id of its own, so suppressing that too would mean
        // the store had stopped distinguishing "sent twice" from "happened twice".
        waitFor(() -> billing.charges().size() == 2);
        sleep(Duration.ofSeconds(2));
        assertThat(billing.charges()).hasSize(2);
    }

    /**
     * The same helper apps/01 uses, and for the same reason: one dependency fewer in an
     * example, and a failure that says the system did not get there rather than naming a
     * library nobody in this repository has otherwise heard of.
     */
    private static void waitFor(java.util.function.BooleanSupplier done) {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the application did not reach the expected state in time");
            }
            sleep(Duration.ofMillis(50));
        }
    }

    private static JdbcDataSource database(String name) {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + name + "-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        return dataSource;
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }

    @SuppressWarnings("unused")
    private static String text(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
