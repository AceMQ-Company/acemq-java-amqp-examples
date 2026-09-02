package org.acemq.examples.apps.policy.underwriting;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.examples.apps.policy.contracts.Policies;

/**
 * Deciding whether to accept an application, and at what price.
 *
 * <p>Underwriting is the one part of this application that is genuinely a *sequence*: check the
 * applicant against the register, price the risk, then decide. Each stage can fail for its own
 * reasons and each is slow for its own reasons, which is what a
 * {@link org.acemq.amqp.core.Pipeline} is for — a queue per stage, so a slow stage shows up as
 * a deep queue you can point at, and can be retried and scaled without touching the others.
 *
 * <p>Written as one consumer doing three things in order, all of that disappears: the whole
 * thing is one queue, one failure mode, and one number that says "underwriting is slow".
 *
 * <p>Each step is <strong>described</strong> as well as named. The name is the routing key and
 * the queue suffix, so it has to stay short and stable; renaming one strands whatever is in
 * flight against the old name. The description is free text and appears in the log line the
 * application prints on start-up, which is where somebody unfamiliar with this system finds out
 * what the stages actually do.
 */
public final class UnderwritingModule implements AutoCloseable {

    /** Applications above this are a human's decision, not a rule's. */
    private static final int REFERRAL_THRESHOLD = 500_000;

    private final org.acemq.amqp.core.Pipeline<Policies.ApplicationSubmitted> pipeline;
    private final MessageConsumer submissions;
    private final AceMq mq;
    private final AtomicInteger accepted = new AtomicInteger();
    private final AtomicInteger declined = new AtomicInteger();

    public UnderwritingModule(AceMq mq) {
        this.mq = mq;

        this.pipeline = mq.pipeline("underwriting", Policies.ApplicationSubmitted.class)
                .step("register", Checked.class, message -> checkRegister(message.payload()))
                        .describedAs("look the applicant up on the shared industry register")
                        // The register is somebody else's service and is periodically
                        // unavailable, which is a wait rather than a decline.
                        .withRetry(RetryPolicy.exponential(4, Duration.ofMillis(200), Duration.ofSeconds(5)))
                .step("price", Priced.class, message -> price(message.payload()))
                        .describedAs("apply the rating table for the product and the applicant's age")
                .step("decide", Void.class, message -> decide(message.payload()))
                        .describedAs("accept, or refer anything a rule should not be deciding")
                .build();

        // The pipeline is fed from the module's own queue rather than being bound to the
        // exchange itself: the pipeline owns its stages' queues, and what enters it is this
        // module's decision.
        this.submissions = mq.consume(
                Policies.UNDERWRITING,
                Policies.ApplicationSubmitted.class,
                message -> pipeline.send(message.payload()));
    }

    /** What the register stage produces. */
    public record Checked(Policies.ApplicationSubmitted application, boolean knownToRegister) { }

    /** What the pricing stage produces. */
    public record Priced(Policies.ApplicationSubmitted application, int annualPremium, boolean refer) { }

    private Checked checkRegister(Policies.ApplicationSubmitted application) {
        // A real one calls an industry service. The interesting part for this example is that
        // it is the stage most likely to be slow, and it has its own queue to prove it.
        return new Checked(application, application.applicant().toLowerCase().contains("known"));
    }

    private Priced price(Checked checked) {
        Policies.ApplicationSubmitted application = checked.application();

        // A rating table, compressed to one line. Older applicants and larger sums cost more.
        int base = application.sumAssured() / 1000;
        int ageLoading = Math.max(0, application.ageOfApplicant() - 30) * 2;
        int registerLoading = checked.knownToRegister() ? base / 2 : 0;
        int premium = base + ageLoading + registerLoading;

        return new Priced(application, premium, application.sumAssured() > REFERRAL_THRESHOLD);
    }

    private Void decide(Priced priced) {
        Policies.ApplicationSubmitted application = priced.application();
        Envelope envelope = Envelope.of("UnderwritingDecision")
                .correlationId(application.applicationId())
                .build();

        if (priced.refer()) {
            // Declined here rather than parked, because "a human must look at this" is a real
            // outcome of underwriting and not a failure of it. Modelling it as an error would
            // put it in a dead-letter queue, where it would look like something broke.
            declined.incrementAndGet();
            mq.publisher(Policies.EXCHANGE, Policies.APPLICATION_DECLINED, Policies.ApplicationDeclined.class)
                    .send(new Policies.ApplicationDeclined(
                            application.applicationId(),
                            application.applicant(),
                            "sum assured of " + application.sumAssured() + " is above the automatic limit"),
                            envelope);
            return null;
        }

        accepted.incrementAndGet();
        mq.publisher(Policies.EXCHANGE, Policies.APPLICATION_ACCEPTED, Policies.ApplicationAccepted.class)
                .send(new Policies.ApplicationAccepted(
                        application.applicationId(),
                        application.applicant(),
                        application.product(),
                        application.sumAssured(),
                        priced.annualPremium()),
                        envelope);
        return null;
    }

    /** @return how many applications underwriting accepted */
    public int accepted() {
        return accepted.get();
    }

    /** @return how many it referred or refused */
    public int declined() {
        return declined.get();
    }

    @Override
    public void close() throws Exception {
        submissions.close();
        pipeline.close();
    }
}
