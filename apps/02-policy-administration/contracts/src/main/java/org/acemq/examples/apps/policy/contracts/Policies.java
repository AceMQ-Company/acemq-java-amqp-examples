package org.acemq.examples.apps.policy.contracts;

import java.util.Map;

import org.acemq.amqp.api.Topology;

/**
 * What every module in this monolith agrees on, and nothing else.
 *
 * <p>The same role `fulfilment-contracts` plays in apps/01, and the reason is sharper here:
 * these modules run in one JVM, so nothing but the build stops one from reaching into
 * another's classes. This jar is the only thing they share, and every other module depends
 * on it and on none of its siblings.
 *
 * <p>That is what makes the monolith <em>modular</em> rather than merely large. A module
 * that only ever received events can be lifted into its own process by changing where it
 * connects — no call sites to find, because there are none.
 *
 * <p>What is deliberately not here: any module's domain model, any persistence, any shared
 * helper. Those are what turn a contracts jar into a common-utils jar, and a common-utils
 * jar is how modular monoliths become ordinary ones.
 */
public final class Policies {

    private Policies() { }

    /** One topic exchange for the whole application. */
    public static final String EXCHANGE = "policy";

    // ---- events -------------------------------------------------------------------

    /** A broker submitted an application. Published by policies, from its outbox. */
    public record ApplicationSubmitted(
            String applicationId, String applicant, String product, int sumAssured, int ageOfApplicant) { }

    /** Underwriting reached a decision and priced it. */
    public record ApplicationAccepted(
            String applicationId, String applicant, String product, int sumAssured, int annualPremium) { }

    /** Underwriting refused it, with a reason a human can act on. */
    public record ApplicationDeclined(String applicationId, String applicant, String reason) { }

    /** A policy exists. Published by policies once underwriting accepted. */
    public record PolicyIssued(
            String policyId, String applicationId, String applicant, String product, int annualPremium) { }

    /**
     * A document belongs to a policy.
     *
     * <p>The document itself is <strong>not</strong> here. This carries a claim check — the
     * key it was stored under and how big it is — because a medical report scanned at
     * 300 dpi is tens of megabytes and a broker is not a filesystem.
     */
    public record DocumentStored(String policyId, String documentKey, String kind, int bytes) { }

    /** The first premium was taken. Published by billing. */
    public record PremiumCharged(String policyId, String applicant, int amount) { }

    /** Somebody claimed against a policy. */
    public record ClaimSubmitted(String claimId, String policyId, int amount, String description) { }

    /** The claim was assessed. */
    public record ClaimSettled(String claimId, String policyId, int paid) { }

    /** It was not, and why. */
    public record ClaimRejected(String claimId, String policyId, String reason) { }

    // ---- routing keys -------------------------------------------------------------

    public static final String APPLICATION_SUBMITTED = "policy.application.submitted";
    public static final String APPLICATION_ACCEPTED = "policy.application.accepted";
    public static final String APPLICATION_DECLINED = "policy.application.declined";
    public static final String POLICY_ISSUED = "policy.policy.issued";
    public static final String DOCUMENT_STORED = "policy.document.stored";
    public static final String PREMIUM_CHARGED = "policy.premium.charged";
    public static final String CLAIM_SUBMITTED = "policy.claim.submitted";
    public static final String CLAIM_SETTLED = "policy.claim.settled";
    public static final String CLAIM_REJECTED = "policy.claim.rejected";

    // ---- queues -------------------------------------------------------------------
    //
    // A queue per module, named for the module. Identical to apps/01, and for the
    // identical reason: two modules wanting the same event each get their own copy.
    // That these queues happen to be served by threads in one process is an operational
    // detail, not an architectural one.

    public static final String UNDERWRITING = "policy.underwriting";
    public static final String POLICIES = "policy.policies";
    public static final String BILLING = "policy.billing";
    public static final String CLAIMS = "policy.claims";

    /**
     * Everything, for the audit trail.
     *
     * <p>Not decoration. Writing this example without it produced a real failure: claims and
     * documents published events that nothing was bound to, and AceMQ refused the publish
     * rather than discarding it — "nothing is bound to exchange 'policy' for routing key
     * 'policy.claim.settled'". A regulated insurer has this queue whatever else it has, and
     * its absence was a bug in the topology rather than a missing feature.
     */
    public static final String AUDIT = "policy.audit";

    /** Where claims asks policies whether a policy is in force. Request/reply, not an event. */
    public static final String POLICY_LOOKUP = "policy.lookup";

    /** What claims asks. */
    public record PolicyQuery(String policyId) { }

    /** What policies answers. A record rather than a boolean, so it can grow a reason. */
    public record PolicyStatus(String policyId, boolean inForce, int annualPremium) { }

    /**
     * The whole application's topology, as one value.
     *
     * <p>Applied once at start-up. In apps/01 every service applied this because none of
     * them could depend on another having started; here there is one process, so it is
     * applied once — and the topology is still declared in the shared jar rather than
     * assembled from each module's fragment, because a module that declares its own queue
     * is a module that can be started against an exchange nobody created.
     *
     * @return what must exist for this application to work
     */
    public static Topology topology() {
        return Topology.define()
                .exchange(EXCHANGE, "topic")

                // Underwriting acts on submissions.
                .classicQueue(UNDERWRITING, Map.of())
                .bind(UNDERWRITING, EXCHANGE, APPLICATION_SUBMITTED)

                // Policies issues once underwriting has accepted, and answers lookups.
                .classicQueue(POLICIES, Map.of())
                .bind(POLICIES, EXCHANGE, APPLICATION_ACCEPTED)

                // Billing charges once a policy exists, never before: charging for a
                // policy that was never issued is a refund and an apology.
                .classicQueue(BILLING, Map.of())
                .bind(BILLING, EXCHANGE, POLICY_ISSUED)

                // Claims needs to know which policies exist.
                .classicQueue(CLAIMS, Map.of())
                .bind(CLAIMS, EXCHANGE, POLICY_ISSUED)

                // Everything, in order, for as long as the regulator asks for. A wildcard
                // binding also means a new event type is audited the day it is introduced
                // rather than the day somebody remembers to add it here.
                .classicQueue(AUDIT, Map.of())
                .bind(AUDIT, EXCHANGE, "policy.#")

                // The request/reply queue claims sends questions to. Not bound to the
                // exchange: a request is addressed to a queue, not routed to whoever
                // happens to be listening.
                .classicQueue(POLICY_LOOKUP, Map.of())

                .build();
    }
}
