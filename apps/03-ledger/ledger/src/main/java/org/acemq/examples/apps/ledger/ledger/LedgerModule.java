package org.acemq.examples.apps.ledger.ledger;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.api.Publisher;
import org.acemq.examples.apps.ledger.contracts.Ledger;

/**
 * The only thing allowed to append to the journal.
 *
 * <p>One writer, deliberately. A ledger's invariant — every transfer produces two entries that
 * sum to zero — cannot be enforced by two processes appending independently, and a stream will
 * happily accept an unbalanced pair from each of them. Making the writer singular is what makes
 * the invariant checkable at all.
 *
 * <p>Balances are <strong>not</strong> stored here. This module decides whether a transfer is
 * allowed and appends the entries; what the balances then are is a question for a projection,
 * which is a different module and a different concern. Keeping a balance here would mean two
 * places that can disagree, and the one that is wrong is always the one somebody trusts.
 */
public final class LedgerModule implements AutoCloseable {

    /**
     * How long the journal keeps entries.
     *
     * <p>An hour, because this is an example that must run in a test. A real ledger keeps them
     * for as long as the law says, which is years, and this is the setting people get wrong:
     * a stream that expires entries is a ledger that quietly loses the ability to rebuild the
     * balances it claims are derived. If the retention is shorter than "forever", the
     * projection is the system of record after all and nobody wrote that down.
     */
    private static final Duration RETENTION = Duration.ofHours(1);

    private static final long MAX_BYTES = 50L * 1024 * 1024;

    private final AceMq mq;
    private final MessageConsumer commands;
    private final Publisher<Ledger.EntryPosted> journal;
    private final Balances balances;
    private final AtomicLong posted = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();

    public LedgerModule(AceMq mq) {
        this.mq = mq;
        mq.declareStream(Ledger.JOURNAL, RETENTION, MAX_BYTES);

        // Published straight at the stream by name. A stream is addressed as a queue, so the
        // default exchange and the stream's name is the whole of it.
        this.journal = mq.publisher("", Ledger.JOURNAL, Ledger.EntryPosted.class);

        // The writer's own view of the balances, rebuilt from the journal on start-up. Not a
        // cache of somebody else's state: it is derived here, from the log, for the one
        // decision this module has to make.
        this.balances = Balances.rebuiltFrom(mq);

        this.commands = mq.consume(Ledger.COMMANDS, Ledger.Transfer.class, message -> apply(message.payload()));
    }

    private void apply(Ledger.Transfer transfer) {
        long available = balances.of(transfer.from());
        if (transfer.amountMinor() <= 0) {
            reject(transfer, "a transfer must be for a positive amount");
            return;
        }
        if (available < transfer.amountMinor()) {
            // Refused, and the refusal is recorded. A ledger that silently drops what it will
            // not do cannot explain itself later.
            reject(transfer, "insufficient funds: " + transfer.from() + " holds " + available);
            return;
        }

        Envelope envelope = Envelope.of("EntryPosted").correlationId(transfer.transferId()).build();

        // Two entries, one transfer, summing to zero. Appended one after the other by the only
        // writer there is, which is what makes "they are both there or neither is" true enough
        // for this example -- a real ledger appends them as one record for exactly this reason,
        // and that is the honest limitation of doing it this way.
        post(new Ledger.EntryPosted(entryId(), transfer.transferId(), transfer.from(),
                -transfer.amountMinor(), transfer.description()), envelope);
        post(new Ledger.EntryPosted(entryId(), transfer.transferId(), transfer.to(),
                transfer.amountMinor(), transfer.description()), envelope);
    }

    private void post(Ledger.EntryPosted entry, Envelope envelope) {
        journal.send(entry, envelope);
        balances.apply(entry);
        posted.incrementAndGet();
    }

    private void reject(Ledger.Transfer transfer, String reason) {
        rejected.incrementAndGet();
        mq.publisher(Ledger.EXCHANGE, Ledger.TRANSFER_REJECTED, Ledger.TransferRejected.class)
                .send(new Ledger.TransferRejected(
                        transfer.transferId(), transfer.from(), transfer.to(), transfer.amountMinor(), reason),
                        Envelope.of("TransferRejected").correlationId(transfer.transferId()).build());
    }

    /** Opens an account with money in it, which every ledger needs a way to do. */
    public void fund(String account, long amountMinor) {
        Ledger.EntryPosted entry = new Ledger.EntryPosted(
                entryId(), "OPENING-" + account, account, amountMinor, "opening balance");
        post(entry, Envelope.of("EntryPosted").correlationId(entry.transferId()).build());
    }

    /** @return this module's own view, derived from the log */
    public long balanceOf(String account) {
        return balances.of(account);
    }

    /** @return entries appended */
    public long posted() {
        return posted.get();
    }

    /** @return transfers refused */
    public long rejected() {
        return rejected.get();
    }

    private static String entryId() {
        return "E-" + UUID.randomUUID();
    }

    @Override
    public void close() throws Exception {
        commands.close();
        balances.close();
    }
}
