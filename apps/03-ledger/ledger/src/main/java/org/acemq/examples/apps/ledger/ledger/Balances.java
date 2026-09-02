package org.acemq.examples.apps.ledger.ledger;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.StreamConsumer;
import org.acemq.examples.apps.ledger.contracts.Ledger;

/**
 * Balances, computed by reading the journal from the beginning.
 *
 * <p>This is what event sourcing buys, and it is worth being concrete: this object holds no
 * state that anybody wrote down. Delete it, restart the process, and it comes back identical,
 * because it is a function of the log and nothing else.
 *
 * <pre>{@code
 * Balances balances = Balances.rebuiltFrom(mq);   // reads from offset zero
 * }</pre>
 *
 * <p>{@code fromFirst()} is the whole trick. A queue cannot do this — reading it consumes it, so
 * the second reader finds nothing. A stream is not emptied by being read, so a projection written
 * next year still gets everything that happened this year.
 *
 * <h2>Read to the end, then stop</h2>
 *
 * <p>The reader is closed once it has caught up, and the writer maintains the balances itself
 * from then on. That is not an optimisation, it is a correctness requirement: the first version
 * of this class kept following the stream <em>and</em> applied each entry as it was written, so
 * every entry the writer posted was counted twice — once locally and once when it came back
 * round. The bug is invisible until an account has a balance twice what it should.
 *
 * <p>Keeping only the stream and dropping the local update has the opposite problem: a transfer
 * is decided against a balance that does not yet include the transfer before it. For a single
 * writer, maintaining its own total after the rebuild is both correct and immediate. It works
 * <em>because</em> there is one writer, which is the reason {@link LedgerModule} insists on that.
 *
 * <h2>The cost</h2>
 *
 * <p>A rebuild is O(history). At a billion entries that stops being free and the answer is a
 * snapshot — "the balance at offset N", plus everything after N. A real technique, deliberately
 * not here: it is the second thing to build, and pretending otherwise makes event sourcing look
 * cheaper than it is.
 */
final class Balances implements AutoCloseable {

    /**
     * How long without an entry counts as "caught up".
     *
     * <p>A crude way to detect the end of a stream and honest about it. The precise way is to
     * read the offset of the last entry before starting and stop there; that is what a
     * production rebuild does, and it needs an offset this example does not otherwise use.
     */
    private static final Duration QUIET_PERIOD = Duration.ofMillis(400);

    private static final Duration REBUILD_LIMIT = Duration.ofSeconds(30);

    private final Map<String, AtomicLong> accounts = new ConcurrentHashMap<>();

    private Balances() { }

    /**
     * @param mq the connection
     * @return balances caught up with everything already in the journal
     */
    static Balances rebuiltFrom(AceMq mq) {
        Balances balances = new Balances();
        AtomicLong lastSeen = new AtomicLong(System.nanoTime());
        AtomicLong replayed = new AtomicLong();

        try (StreamConsumer reader = mq.stream(Ledger.JOURNAL, Ledger.EntryPosted.class)
                .fromFirst()
                .consume(message -> {
                    balances.apply(message.payload());
                    replayed.incrementAndGet();
                    lastSeen.set(System.nanoTime());
                })) {

            long deadline = System.nanoTime() + REBUILD_LIMIT.toNanos();
            while (System.nanoTime() - lastSeen.get() < QUIET_PERIOD.toNanos()) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("the journal did not stop producing entries within "
                            + REBUILD_LIMIT + "; a rebuild cannot finish while somebody is still writing");
                }
                sleep();
            }
        } catch (Exception e) {
            throw new IllegalStateException("could not rebuild balances from " + Ledger.JOURNAL, e);
        }
        return balances;
    }

    /** Applied by the writer as it appends, which is safe precisely because there is one writer. */
    void apply(Ledger.EntryPosted entry) {
        accounts.computeIfAbsent(entry.account(), key -> new AtomicLong()).addAndGet(entry.amountMinor());
    }

    long of(String account) {
        AtomicLong balance = accounts.get(account);
        return balance == null ? 0L : balance.get();
    }

    private static void sleep() {
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while rebuilding balances", e);
        }
    }

    @Override
    public void close() {
        // The reader is already closed; the balances are ordinary memory.
    }
}
