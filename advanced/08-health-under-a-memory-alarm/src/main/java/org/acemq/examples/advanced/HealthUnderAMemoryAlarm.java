package org.acemq.examples.advanced;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.actuator.AceMqActuator;
import org.acemq.amqp.actuator.ActuatorOptions;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.transport.ConnectionBlockedException;
import org.acemq.amqp.transport.ConnectionConfig;

/**
 * What {@code /acemq-health} reports while the broker is refusing to read this connection.
 *
 * <p>RabbitMQ raises an alarm when memory or disk crosses a watermark, tells the connections
 * publishing on it that they are blocked, and then stops reading them. The endpoint answers
 * <strong>UP</strong> on 200, with {@code blocked: true} and the broker's own reason — and
 * answers without asking the broker anything, because a broker that has stopped reading will
 * not answer a probe either.
 *
 * <p>Both halves are shown here against a real alarm, and the second is what gives the first
 * any meaning. A health check that is fast against a broker which was never blocked has
 * demonstrated nothing, so this also starts an ordinary round trip on that same connection
 * and shows it still waiting five seconds later.
 *
 * <p>This is the counterpart to {@code intermediate/05-blocked-connections}, which shows what
 * an alarm does to a <em>publisher</em> and does it on the in-memory transport, with no Docker
 * and no alarm. This one is about what an <em>orchestrator</em> is told, and nothing but a
 * real broker can answer that.
 *
 * <h2>The alarm, and putting it back</h2>
 *
 * <p>{@code rabbitmqctl set_vm_memory_high_watermark 0} is a real alarm on a real node. The
 * watermark goes back to the image's default of 0.4 in a {@code finally}, so that a failed run
 * does not leave every publisher on that broker refused until somebody notices — and so that
 * the connection is not closed while blocked, which waits on a broker that is not reading.
 *
 * <p>The alarm belongs to the node rather than to this connection, so the broker has to be one
 * nothing else is using. {@code docker-compose.yml} here starts one; the test starts its own.
 *
 * <pre>{@code
 * docker compose up -d
 * mvn compile exec:java
 * }</pre>
 */
public final class HealthUnderAMemoryAlarm {

    /** The queue the control round trip asks about. Nothing is consumed from it. */
    private static final String QUEUE = "orders.new";

    /**
     * How long a publish waits for the alarm to clear before giving up.
     *
     * <p>Two seconds so the refusal below is quick to watch. The default is thirty, and
     * whatever you choose you should choose: it decides how long a request thread sits in a
     * publish while the broker is in alarm. See {@code intermediate/05-blocked-connections}.
     */
    private static final Duration BLOCKED_TIMEOUT = Duration.ofSeconds(2);

    /**
     * What the five health facts have to be read within.
     *
     * <p>The measurement is in microseconds and the release notes say under a millisecond, so
     * this is a hundred times looser than the claim. Loose on purpose: a threshold tight
     * enough to be impressive goes red on a busy machine for a reason that has nothing to do
     * with the library, and what proves no round trip was made is the round trip further down
     * that never returns, not this number.
     */
    private static final Duration FACTS_BUDGET = Duration.ofMillis(100);

    /** What the rabbitmq image ships with, and what this puts back. */
    private static final String DEFAULT_WATERMARK = "0.4";

    public record Order(String id) { }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");
        // Everything after the URL is the command that reaches rabbitmqctl on the broker's
        // node -- "docker exec <container> rabbitmqctl", or just "rabbitmqctl" where this runs
        // beside the broker. Taken as words rather than as one quoted string so that running
        // it by hand does not turn into an argument about shell quoting.
        List<String> rabbitmqctl = args.length > 1
                ? Arrays.asList(args).subList(1, args.length)
                : List.of("docker", "exec", "acemq-alarm-broker", "rabbitmqctl");

        try (AceMq mq = AceMq.connect(ConnectionConfig.url(url)
                        .blockedTimeout(BLOCKED_TIMEOUT)
                        .build());
                AceMqActuator actuator = AceMqActuator.start(ActuatorOptions.builder()
                        .connection(mq)
                        // Loopback, and an ephemeral port. Nothing here is authenticated, and
                        // a fixed port is how two things that both want it fail depending on
                        // which started first.
                        .port(0)
                        .application("orders", "1.0.0")
                        .build())) {
            mq.declareQueue(QUEUE);
            System.out.printf("  serving    %s on %s%n", actuator.paths(), actuator.url());

            // A healthy broker first, so the numbers below have something to be read against.
            System.out.printf("  before     %s%n", get(actuator, "/acemq-health"));

            System.out.println();
            System.out.println("  alarm      dropping the memory high watermark to nothing, which is");
            System.out.println("             the state a production broker reaches under memory pressure");
            run(rabbitmqctl, "set_vm_memory_high_watermark", "0");
            try {
                blockAndReport(mq, actuator);
            } finally {
                // Before the connection closes, which is the only order that works: closing a
                // blocked connection waits on a broker that has stopped reading.
                System.out.printf("  restored   the watermark is back at %s%n", DEFAULT_WATERMARK);
                run(rabbitmqctl, "set_vm_memory_high_watermark", DEFAULT_WATERMARK);
                awaitUnblocked(mq);
            }

            System.out.printf("  after      %s%n", get(actuator, "/acemq-health"));
            mq.deleteQueue(QUEUE);
        }
    }

    private static void blockAndReport(AceMq mq, AceMqActuator actuator) throws Exception {
        // The alarm on its own tells this connection nothing. RabbitMQ sends the blocked
        // notification to a connection that publishes under an alarm, so a publisher finds out
        // by publishing and an idle connection does not find out at all -- which is exactly
        // why a health check has to be able to answer the question without publishing.
        provokeTheBlock(mq);
        awaitBlocked(mq);

        // Exactly the facts the health endpoint and the Spring Boot indicator read, in the
        // order they read them. None of them asks the broker anything.
        long startedAt = System.nanoTime();
        boolean open = mq.isOpen();
        String transport = mq.transportName();
        boolean blocked = mq.isBlocked();
        long inFlight = mq.inFlight();
        Optional<String> reason = mq.blockedReason();
        Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

        System.out.printf("  facts      open=%s transport=%s blocked=%s inFlight=%d reason=%s%n",
                open, transport, blocked, inFlight, reason.orElse("(none)"));
        System.out.printf("  facts      read in %d us%n", took.toNanos() / 1000);

        // A blocked connection is open: the broker is talking to this socket, it is just not
        // reading from it.
        require(open, "a blocked connection should still be open");
        require(blocked, "the broker should have blocked this connection");
        // The broker's own words, which is what belongs in the alert. "low on memory" is
        // actionable; "the publish failed" is not.
        require(reason.isPresent() && reason.orElseThrow().contains("memory"),
                "the reason should be the broker's own, and it reads " + reason.orElse("(none)"));
        require(took.compareTo(FACTS_BUDGET) < 0,
                "reading the health facts took " + took + ", which is long enough to be a round trip");

        // UP on 200, not DOWN on 503. A blocked connection is the broker protecting itself
        // from a memory or disk alarm, and an application that fails its own readiness check
        // for it is one an orchestrator restarts into the same blocked broker, having thrown
        // away whatever it was holding. A fleet doing that together stops draining the queues
        // at the moment the broker most needs them drained -- and consumers are unaffected by
        // an alarm, so the instance that stays up is the one that can help.
        String health = get(actuator, "/acemq-health");
        System.out.printf("  during     %s%n", health);
        require(health.startsWith("200 "), "a blocked broker must not fail the readiness probe: " + health);
        require(health.contains("\"status\":\"UP\""), "a blocked connection is UP: " + health);
        require(health.contains("\"blocked\":true"), "the endpoint should say so: " + health);
        require(health.contains("\"blockedReason\""), "and should say why: " + health);

        // What publishing does now. Refused with the reason after the blocked timeout, rather
        // than silently swallowed -- see intermediate/05 for what that number costs.
        long waitedFrom = System.nanoTime();
        try {
            mq.publisher("", QUEUE, Order.class).send(new Order("o-2"));
            throw new IllegalStateException("a publish succeeded on a blocked connection");
        } catch (ConnectionBlockedException e) {
            System.out.printf("  publish    refused after %d s: reason=%s mayHaveBeenPublished=%s%n",
                    Duration.ofNanos(System.nanoTime() - waitedFrom).toSeconds(),
                    e.reason(), e.mayHaveBeenPublished());
        }

        assertARoundTripWouldHaveHung(mq);
    }

    /**
     * The control, and the reason anything above means anything.
     *
     * <p>A passive queue declare is the cheapest question this library asks a broker, and it is
     * the shape of every health check that hung. On a blocked connection it is not refused: it
     * goes unanswered, for as long as the alarm lasts.
     */
    private static void assertARoundTripWouldHaveHung(AceMq mq) throws InterruptedException {
        CountDownLatch answered = new CountDownLatch(1);
        Thread probe = new Thread(
                () -> {
                    try {
                        mq.messageCount(QUEUE);
                    } catch (Throwable ignored) {
                        // How it ends does not matter. That it ends at all is the measurement.
                    } finally {
                        answered.countDown();
                    }
                },
                "would-be-health-probe");
        probe.setDaemon(true);
        probe.start();

        boolean returned = answered.await(5, TimeUnit.SECONDS);
        System.out.printf("  control    a queue lookup on this same connection has returned after 5s: %s%n",
                returned);
        require(!returned,
                "a round trip answered on a blocked connection, so nothing above was measured against a block");
    }

    /**
     * Publishes on a thread of its own, because this is the publish that discovers the alarm
     * and it parks in the broker's silence until the blocked timeout runs out.
     */
    private static void provokeTheBlock(AceMq mq) {
        Thread publisher = new Thread(
                () -> {
                    try {
                        mq.publisher("", QUEUE, Order.class).send(new Order("o-1"));
                    } catch (Throwable ignored) {
                        // Expected: the broker stopped reading with this one in flight.
                    }
                },
                "blocked-publisher");
        publisher.setDaemon(true);
        publisher.start();
    }

    private static void awaitBlocked(AceMq mq) throws InterruptedException {
        if (!await(() -> mq.isBlocked(), Duration.ofSeconds(60))) {
            throw new IllegalStateException(
                    "the broker never blocked this connection, so there was nothing to measure");
        }
        System.out.printf("  blocked    the broker said so: %s%n", mq.blockedReason().orElse("(none)"));
    }

    private static void awaitUnblocked(AceMq mq) throws InterruptedException {
        if (!await(() -> !mq.isBlocked(), Duration.ofSeconds(60))) {
            throw new IllegalStateException("the alarm did not clear");
        }
    }

    private static boolean await(java.util.function.BooleanSupplier condition, Duration limit)
            throws InterruptedException {
        long deadline = System.nanoTime() + limit.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(100);
        }
        return condition.getAsBoolean();
    }

    /** Fetches a path from the actuator and renders it as "&lt;status&gt; &lt;body&gt;". */
    private static String get(AceMqActuator actuator, String path) throws IOException {
        HttpURLConnection http = (HttpURLConnection) URI.create(actuator.url() + path).toURL().openConnection();
        http.setConnectTimeout(5_000);
        http.setReadTimeout(5_000);
        try {
            int status = http.getResponseCode();
            InputStream body = status < 400 ? http.getInputStream() : http.getErrorStream();
            return status + " " + new String(body.readAllBytes(), StandardCharsets.UTF_8).trim();
        } finally {
            http.disconnect();
        }
    }

    /** Runs rabbitmqctl on the broker's node, through whatever command was supplied. */
    private static void run(List<String> prefix, String... arguments) throws Exception {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command().addAll(prefix);
        builder.command().addAll(Arrays.asList(arguments));
        builder.redirectErrorStream(true);

        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
            process.destroyForcibly();
            throw new IllegalStateException(
                    "could not run " + String.join(" ", builder.command()) + ": " + output.trim());
        }
    }

    private static void require(boolean claim, String complaint) {
        if (!claim) {
            throw new IllegalStateException(complaint);
        }
    }
}
