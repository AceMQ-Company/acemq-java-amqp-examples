package org.acemq.examples.apps.policy.claims;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.core.RequestTimedOutException;
import org.acemq.amqp.core.Requester;
import org.acemq.examples.apps.policy.contracts.Policies;

/**
 * Claims: the module that has to ask another module a question.
 *
 * <p>Everything else in this application reacts to events, which is the right default. Claims
 * cannot: before settling a claim it must know whether the policy is in force, and it needs the
 * answer <em>now</em>, in the middle of a decision. An event cannot answer a question.
 *
 * <p>So it asks, over the broker, with {@link Requester} — and this is the case
 * {@code request-reply.md} says the pattern is genuinely for, with a twist. The callee is in
 * the same JVM. A method call would work today and would be the wrong choice: the moment claims
 * calls into policies directly, the two are one module and the boundary that makes this a
 * modular monolith is gone. Asking over the broker costs a millisecond and keeps the seam.
 *
 * <p>The timeout is the part not to skip. A request that waits forever is how one slow module
 * stops the whole application, monolith or not.
 */
public final class ClaimsModule implements AutoCloseable {

    /** Generous for an in-process hop, and still bounded. */
    private static final Duration LOOKUP_TIMEOUT = Duration.ofSeconds(5);

    private final AceMq mq;
    private final Requester requester;
    private final MessageConsumer issued;
    private final AtomicInteger settled = new AtomicInteger();
    private final AtomicInteger rejected = new AtomicInteger();

    public ClaimsModule(AceMq mq) {
        this.mq = mq;
        this.requester = mq.requester();

        // Claims listens for issued policies only to know they exist at all; the authoritative
        // answer still comes from the lookup, because a policy can be cancelled after issue and
        // this module deliberately does not keep a copy of another module's state.
        this.issued = mq.consume(Policies.CLAIMS, Policies.PolicyIssued.class, message -> { });
    }

    /**
     * Assesses a claim against a policy.
     *
     * @param policyId the policy claimed against
     * @param amount how much is claimed
     * @param description what happened
     * @return the claim id
     */
    public String submit(String policyId, int amount, String description) {
        String claimId = "CLM-" + UUID.randomUUID().toString().substring(0, 8);
        Envelope envelope = Envelope.of("Claim").correlationId(policyId).build();

        Policies.PolicyStatus status;
        try {
            status = requester.request(
                    "", Policies.POLICY_LOOKUP, new Policies.PolicyQuery(policyId),
                    Policies.PolicyStatus.class, LOOKUP_TIMEOUT);
        } catch (RequestTimedOutException e) {
            // Not an answer, and must not be treated as "no". Refusing a valid claim because a
            // lookup was slow is the failure mode worth being explicit about.
            throw new IllegalStateException(
                    "could not establish whether " + policyId + " is in force, so claim " + claimId
                            + " was neither settled nor rejected; it must be retried", e);
        }

        if (!status.inForce()) {
            rejected.incrementAndGet();
            mq.publisher(Policies.EXCHANGE, Policies.CLAIM_REJECTED, Policies.ClaimRejected.class)
                    .send(new Policies.ClaimRejected(claimId, policyId, "no policy in force"), envelope);
            return claimId;
        }

        // A real assessment is a great deal more than this. What matters here is that it
        // happened after an authoritative answer rather than after a guess.
        settled.incrementAndGet();
        mq.publisher(Policies.EXCHANGE, Policies.CLAIM_SETTLED, Policies.ClaimSettled.class)
                .send(new Policies.ClaimSettled(claimId, policyId, amount), envelope);
        return claimId;
    }

    /** @return claims settled */
    public int settled() {
        return settled.get();
    }

    /** @return claims rejected because no policy was in force */
    public int rejected() {
        return rejected.get();
    }

    @Override
    public void close() throws Exception {
        issued.close();
        requester.close();
    }
}
