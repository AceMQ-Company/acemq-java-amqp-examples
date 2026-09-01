package org.acemq.examples.basic;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.OrderedQueue;

/**
 * Keeping messages for one entity in order, without giving up parallelism.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class OrderedPerKey {

    /** A change to one account. Applying these out of order gives the wrong balance. */
    public record LedgerEntry(String account, int sequence, double amount) { }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        try (AceMq mq = AceMq.connect(url)) {
            Map<String, List<Integer>> applied = new ConcurrentHashMap<>();
            List<String> threads = new CopyOnWriteArrayList<>();

            // Four partitions, keyed by account. Every entry for one account lands in
            // one partition and is handled in sequence; different accounts run in
            // parallel. Four is also the throughput ceiling: partitions are the unit
            // of concurrency here, and changing the count later reshuffles which key
            // goes where.
            try (OrderedQueue<LedgerEntry> ledger = mq.ordered("ledger", LedgerEntry.class)
                    .partitions(4)
                    .keyedBy(LedgerEntry::account)
                    .onFailure(OrderedQueue.OnFailure.STOP)
                    .declare()
                    .consume(message -> {
                        LedgerEntry entry = message.payload();
                        applied.computeIfAbsent(entry.account(), a -> new CopyOnWriteArrayList<>())
                               .add(entry.sequence());
                        threads.add(Thread.currentThread().getName());
                        // Uneven work, so that a slow account cannot accidentally
                        // serialise the others and make the test pass by luck.
                        Thread.sleep(entry.account().equals("acct-a") ? 40 : 5);
                    })) {

                for (int i = 1; i <= 10; i++) {
                    ledger.send(new LedgerEntry("acct-a", i, i));
                    ledger.send(new LedgerEntry("acct-b", i, i));
                    ledger.send(new LedgerEntry("acct-c", i, i));
                }

                waitFor(() -> ledger.handled() == 30, Duration.ofSeconds(60));
            }

            for (String account : new ArrayList<>(new java.util.TreeSet<>(applied.keySet()))) {
                List<Integer> order = new ArrayList<>(applied.get(account));
                List<Integer> sorted = new ArrayList<>(order);
                java.util.Collections.sort(sorted);
                System.out.printf("  %s   %s  in order: %s%n",
                        account, order, order.equals(sorted));
            }

            // The point of partitions rather than one consumer: the accounts were
            // not handled one after another.
            Map<String, Integer> perThread = new LinkedHashMap<>();
            threads.forEach(t -> perThread.merge(t, 1, Integer::sum));
            System.out.printf("  handled by %d threads, so the accounts ran in parallel%n",
                    perThread.size());
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier done, Duration limit) throws Exception {
        long deadline = System.nanoTime() + limit.toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("timed out waiting for the example to progress");
            }
            Thread.sleep(100);
        }
    }
}
