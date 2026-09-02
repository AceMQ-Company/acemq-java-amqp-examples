package org.acemq.examples.apps.ledger.projections;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.StreamConsumer;
import org.acemq.examples.apps.ledger.contracts.Ledger;

/**
 * A statement per account, built by reading the journal.
 *
 * <p>This module exists to make one claim checkable: **a projection is disposable**. It stores
 * nothing the log does not contain, it is built by reading from offset zero, and throwing it
 * away costs nothing but the time to read the log again.
 *
 * <p>It also proves the log is genuinely shared. The ledger module is reading the same stream
 * from the same offset for its own purposes, and neither reader affects the other — no
 * competing consumption, no acknowledgement, no "who got the message". That is the property a
 * queue does not have and the reason a ledger wants a stream.
 *
 * <h2>Adding a projection later is the point</h2>
 *
 * <p>This one keeps statements. A fraud model, a tax report, a daily-balance chart — each is a
 * new reader from offset zero, added without touching the writer, without a migration, and
 * with full history from the day it starts rather than from the day somebody deployed it.
 *
 * <p>That is the argument for event sourcing, and it is worth weighing against the cost: every
 * question you did not anticipate is answerable, and every projection is code you now maintain
 * and rebuild.
 */
public final class StatementProjection implements AutoCloseable {

    private final Map<String, List<Ledger.EntryPosted>> statements = new ConcurrentHashMap<>();
    private final StreamConsumer reader;

    /**
     * @param mq the connection
     * @param fromFirst whether to read all of history, or only what arrives from now
     */
    public StatementProjection(AceMq mq, boolean fromFirst) {
        var stream = mq.stream(Ledger.JOURNAL, Ledger.EntryPosted.class);
        this.reader = (fromFirst ? stream.fromFirst() : stream.fromNext())
                .consume(message -> {
                    Ledger.EntryPosted entry = message.payload();
                    statements
                            .computeIfAbsent(entry.account(), key -> new CopyOnWriteArrayList<>())
                            .add(entry);
                });
    }

    /**
     * @param account whose statement
     * @return the entries seen for it, oldest first
     */
    public List<Ledger.EntryPosted> statementOf(String account) {
        return List.copyOf(statements.getOrDefault(account, List.of()));
    }

    /**
     * @param account whose balance
     * @return the sum of the entries, which is what a balance is
     */
    public long balanceOf(String account) {
        long total = 0;
        for (Ledger.EntryPosted entry : statementOf(account)) {
            total += entry.amountMinor();
        }
        return total;
    }

    /** @return every account this projection has seen an entry for */
    public List<String> accounts() {
        return new ArrayList<>(statements.keySet());
    }

    /** @return entries read */
    public int entries() {
        return statements.values().stream().mapToInt(List::size).sum();
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }
}
