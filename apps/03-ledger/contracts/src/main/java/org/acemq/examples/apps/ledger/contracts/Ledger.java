package org.acemq.examples.apps.ledger.contracts;

import java.util.Map;

import org.acemq.amqp.api.Topology;

/**
 * The events a ledger is made of.
 *
 * <p>In apps/01 and apps/02 the events describe what happened <em>to</em> the system of record.
 * Here they <strong>are</strong> the system of record. There is no balances table that events
 * update; a balance is what you get by adding up entries, and it can be deleted and recomputed
 * without losing anything, because nothing was ever stored that the log does not contain.
 *
 * <p>That is the whole claim, and it has one consequence worth stating before the code: **an
 * entry is never changed and never deleted.** Money moved wrongly is corrected by posting the
 * opposite entry, exactly as a paper ledger does, and both entries stay. A system that edits
 * history cannot answer "what did we believe on Tuesday", which for a ledger is the question
 * auditors actually ask.
 */
public final class Ledger {

    private Ledger() { }

    /**
     * The stream every entry is appended to.
     *
     * <p>A stream rather than a queue, and the difference is the point. A queue is emptied by
     * being read; a stream is not. Ten consumers can each read all of history at their own pace,
     * a new projection can start from offset zero next year, and nothing anybody reads removes
     * anything for anybody else.
     */
    public static final String JOURNAL = "ledger.journal";

    /** Where transfer commands arrive. An ordinary queue: a command is handled once. */
    public static final String COMMANDS = "ledger.commands";

    /** Where the ledger announces what it accepted, for anything that is not a projection. */
    public static final String EXCHANGE = "ledger";

    public static final String ENTRY_POSTED = "ledger.entry.posted";
    public static final String TRANSFER_REJECTED = "ledger.transfer.rejected";

    // ---- the log ------------------------------------------------------------------

    /**
     * One side of one movement of money.
     *
     * <p>Signed rather than a debit/credit flag: a sum over a column is then simply a sum, and
     * "which sign means debit" stops being a question every reader has to answer. Amounts are
     * whole minor units — pennies, cents — because a ledger in {@code double} is a ledger that
     * disagrees with itself after enough additions.
     *
     * @param entryId unique, and the idempotency key
     * @param transferId the movement this is one half of
     * @param account whose balance changes
     * @param amountMinor positive credits the account, negative debits it
     * @param description what a statement will show
     */
    public record EntryPosted(
            String entryId, String transferId, String account, long amountMinor, String description) { }

    /** A transfer that was refused, with the reason kept in the log beside the ones that were not. */
    public record TransferRejected(String transferId, String from, String to, long amountMinor, String reason) { }

    // ---- commands -----------------------------------------------------------------

    /** Move money between two accounts. Not an event: it is a request, and may be refused. */
    public record Transfer(String transferId, String from, String to, long amountMinor, String description) { }

    /**
     * The whole application's topology.
     *
     * @return what must exist for this ledger to work
     */
    public static Topology topology() {
        return Topology.define()
                .exchange(EXCHANGE, "topic")

                // Commands: an ordinary queue, because a transfer must be applied once.
                .classicQueue(COMMANDS, Map.of())
                .bind(COMMANDS, EXCHANGE, "ledger.transfer.requested")

                // Rejections are announced so somebody can act on them. The journal keeps its
                // own copy; this is for whoever wants to be told rather than to read.
                .classicQueue("ledger.rejections", Map.of())
                .bind("ledger.rejections", EXCHANGE, TRANSFER_REJECTED)

                .build();
    }
}
