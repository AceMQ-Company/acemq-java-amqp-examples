package org.acemq.examples.basic;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.acemq.amqp.api.AceFatalException;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;

/**
 * What happens when a handler fails: retries the broker schedules, and a dead-letter queue
 * for the ones that never succeed.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class RetriesAndDeadLetters {

    public record Payment(String id, double amount) { }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        try (AceMq mq = AceMq.connect(url)) {
            mq.declareExchange("payments", "topic");
            mq.declareQueue("payments.new");
            mq.bind("payments.new", "payments", "payment.*");

            // Three attempts, one second apart. The wait happens in the broker: the
            // message is published into a queue with a time-to-live and dead-lettered
            // back when it expires. No thread sleeps, so a retry storm cannot exhaust
            // a pool, and retries survive a restart of this process.
            RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofSeconds(1)).withJitter(0);

            firstSucceedsAfterRetrying(mq, policy);
            thenGivesUpIntoTheDeadLetterQueue(mq, policy);
            andSkipsRetriesEntirelyWhenItCannotHelp(mq, policy);
        }
    }

    /** A downstream blip: the third attempt works, and nothing was lost. */
    private static void firstSucceedsAfterRetrying(AceMq mq, RetryPolicy policy) throws Exception {
        AtomicBoolean downstreamIsBroken = new AtomicBoolean(true);
        List<Integer> attempts = new CopyOnWriteArrayList<>();
        CountDownLatch succeeded = new CountDownLatch(1);

        try (MessageConsumer consumer = mq.consume("payments.new", Payment.class,
                ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    attempts.add(message.attempt());
                    if (downstreamIsBroken.get()) {
                        throw new IllegalStateException("the payment gateway is unreachable");
                    }
                    succeeded.countDown();
                })) {

            mq.publisher("payments", "payment.taken", Payment.class)
              .send(new Payment("p-1", 42.00));

            // Let it fail once, then fix the world underneath it.
            Thread.sleep(1_500);
            downstreamIsBroken.set(false);

            if (!succeeded.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the retry never succeeded");
            }
            System.out.printf("  recovered  after attempts %s, retried %d time(s)%n",
                    attempts, consumer.retried());
        }
    }

    /** Nothing ever works: the message ends up somewhere you can find it. */
    private static void thenGivesUpIntoTheDeadLetterQueue(AceMq mq, RetryPolicy policy) throws Exception {
        try (MessageConsumer consumer = mq.consume("payments.new", Payment.class,
                ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    throw new IllegalStateException("the payment gateway is still unreachable");
                })) {

            mq.publisher("payments", "payment.taken", Payment.class)
              .send(new Payment("p-2", 7.50));

            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (consumer.deadLettered() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(200);
            }
            System.out.printf("  gave up    after %d attempts, %d dead-lettered%n",
                    policy.maxAttempts(), consumer.deadLettered());
        }

        // It is not lost. It is in payments.new.dlq, with the reason on the envelope,
        // and mq.replay("payments.new") puts it back once the cause is fixed.
        System.out.printf("  parked     %d message(s) waiting in payments.new.dlq%n",
                mq.messageCount("payments.new.dlq"));
    }

    /** Some failures cannot be fixed by trying again, and saying so saves three attempts. */
    private static void andSkipsRetriesEntirelyWhenItCannotHelp(AceMq mq, RetryPolicy policy) throws Exception {
        long before = mq.messageCount("payments.new.dlq");

        try (MessageConsumer consumer = mq.consume("payments.new", Payment.class,
                ConsumerOptions.prefetch(1).withRetry(policy), message -> {
                    // A negative amount will be negative on every attempt. AceFatalException
                    // skips the ladder and goes straight to the dead-letter queue.
                    throw new AceFatalException("a payment cannot be negative: " + message.payload().amount());
                })) {

            mq.publisher("payments", "payment.taken", Payment.class)
              .send(new Payment("p-3", -1.00));

            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (consumer.deadLettered() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(200);
            }
            System.out.printf("  refused    a hopeless message after %d attempt, not %d%n",
                    1, policy.maxAttempts());
        }
        System.out.printf("  dlq now    holds %d message(s)%n", mq.messageCount("payments.new.dlq"));
    }
}
