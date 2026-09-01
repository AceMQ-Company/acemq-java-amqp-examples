package org.acemq.examples.intermediate;

import java.time.Duration;
import java.util.UUID;

import org.acemq.amqp.patterns.JdbcIdempotencyStore;
import org.h2.jdbcx.JdbcDataSource;

/**
 * Handling a message once across a fleet, not once per process.
 *
 * <p>An in-memory store makes one instance idempotent. Run four of them and the guarantee
 * is gone: each has its own memory, so the duplicate that gets redelivered to a different
 * instance is handled again. That is the failure that only appears after you scale up, and
 * only under load, which is the worst time to discover it.
 *
 * <p>A shared store is the answer, and the interesting part is that it is a <em>lease</em>
 * rather than a lock.
 *
 * <p>No Docker needed: {@code mvn compile exec:java}.
 */
public final class SharedIdempotency {

    public static void main(String[] args) throws Exception {
        JdbcDataSource database = new JdbcDataSource();
        database.setURL("jdbc:h2:mem:idem-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        database.setUser("sa");

        // Two instances of the same service, each with its own store object, both
        // pointing at one database. This is the shape in production: the store is not
        // shared in memory, it is shared in the database.
        //
        // The claim timeout is how long a claim is honoured before another instance may
        // take it. Two seconds here so the example finishes; minutes in production, and
        // it wants to be comfortably longer than the slowest handler.
        JdbcIdempotencyStore instanceA = new JdbcIdempotencyStore(
                database, Duration.ofHours(1), Duration.ofSeconds(2), "handled_messages");
        JdbcIdempotencyStore instanceB = new JdbcIdempotencyStore(
                database, Duration.ofHours(1), Duration.ofSeconds(2), "handled_messages");
        instanceA.createSchemaIfAbsent();

        String message = "order-42";

        // The broker redelivers, and this time the copy lands on a different instance.
        boolean firstWon = instanceA.claim(message);
        boolean secondWon = instanceB.claim(message);
        System.out.printf("  claim      A=%s B=%s%n", firstWon, secondWon);

        // A wins, does the work, and records that it finished. Between the claim and the
        // confirm is the only window that matters, and it is the handler's duration.
        instanceA.confirm(message);
        System.out.printf("  confirmed  isConfirmed=%s%n", instanceA.isConfirmed(message));

        // A third delivery, weeks later, after both instances have been replaced. The
        // store still knows, which is what retention is for -- and why retention has to
        // outlive the longest redelivery you can imagine, including a replay.
        System.out.printf("  again      claim=%s%n", instanceB.claim(message));

        // Now the part that makes it a lease rather than a lock. An instance that claims
        // a message and dies -- OOM, a pod evicted, a power cut -- never confirms and
        // never releases. A lock would leave that message unhandleable forever, and the
        // only cure would be someone deleting a row by hand at three in the morning.
        String orphan = "order-43";
        System.out.printf("  orphan     claimed by A=%s%n", instanceA.claim(orphan));
        System.out.printf("  orphan     B cannot take it yet=%s%n", !instanceB.claim(orphan));

        Thread.sleep(Duration.ofSeconds(3).toMillis());

        // The claim has expired, so another instance picks the message up. At-least-once
        // is preserved: the work happens, late, rather than never.
        System.out.printf("  orphan     after the lease expires, B claims it=%s%n",
                instanceB.claim(orphan));

        // The deliberate release, for a handler that failed and wants the message tried
        // again immediately rather than after the lease runs out.
        String failed = "order-44";
        instanceA.claim(failed);
        instanceA.release(failed);
        System.out.printf("  released   another instance can claim at once=%s%n",
                instanceB.claim(failed));

        System.out.printf("  rows       %d%n", instanceA.size());
    }
}
