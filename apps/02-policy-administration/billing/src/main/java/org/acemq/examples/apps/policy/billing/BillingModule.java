package org.acemq.examples.apps.policy.billing;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.sql.DataSource;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.patterns.JdbcIdempotencyStore;
import org.acemq.examples.apps.policy.contracts.Policies;

/**
 * Taking the first premium: the one module where handling a message twice is real money.
 *
 * <p>Identical reasoning to the payments service in apps/01, and worth repeating here because
 * the monolith makes it easy to assume the problem went away. It did not. The retry ladder
 * still redelivers, a redeploy mid-handler still leaves a message unacknowledged, and both
 * still produce a second delivery of a message that already took money.
 *
 * <p>The idempotency store is JDBC rather than in-memory even though this is one process,
 * because "one process" is a fact about today. The moment this module is lifted out — which is
 * the whole point of the arrangement — an in-memory store becomes two stores that each think
 * they are the only one, and each charges once.
 */
public final class BillingModule implements AutoCloseable {

    private final MessageConsumer issued;
    private final JdbcIdempotencyStore seen;
    private final List<String> charges = new CopyOnWriteArrayList<>();

    public BillingModule(AceMq mq, DataSource dataSource) {
        this.seen = new JdbcIdempotencyStore(dataSource);
        this.seen.createSchemaIfAbsent();

        this.issued = mq.consume(
                Policies.BILLING,
                Policies.PolicyIssued.class,
                // The store is handed to the consumer rather than used by hand: the claim is
                // taken before the handler and confirmed after it returns, which is the order
                // that closes the window a manual "mark it afterwards" leaves open.
                ConsumerOptions.prefetch(10).idempotent(seen),
                message -> {
                    Policies.PolicyIssued policy = message.payload();
                    charges.add(policy.policyId());
                    mq.publisher(Policies.EXCHANGE, Policies.PREMIUM_CHARGED, Policies.PremiumCharged.class)
                            .send(new Policies.PremiumCharged(
                                    policy.policyId(), policy.applicant(), policy.annualPremium()),
                                    Envelope.of("PremiumCharged").correlationId(policy.applicationId()).build());
                });
    }

    /** @return one entry per charge actually taken; duplicates here would be the bug */
    public List<String> charges() {
        return List.copyOf(charges);
    }

    @Override
    public void close() throws Exception {
        issued.close();
    }
}
