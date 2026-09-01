package org.acemq.examples.intermediate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The only integration test in this repository with no container in it, because the
 * example it runs needs no broker.
 *
 * <p>Named {@code RunsWithoutABrokerIT} rather than {@code TestingWithoutABrokerIT}
 * because surefire's default includes are {@code Test*.java} as well as {@code *Test.java}
 * -- a class beginning with "Testing" is picked up as a unit test and runs twice, once in
 * surefire and once in failsafe. Harmless here and expensive for anything that starts a
 * container.
 */
@DisplayName("intermediate/04 — testing without a broker")
class RunsWithoutABrokerIT {

    @Test
    @Timeout(60)
    void theExampleRunsWithNothingListeningOnPort5672() throws Exception {
        String output = runMain();

        assertThat(output).contains("escalated  [large]");

        // reset() really discards the broker: the queue declared above is gone, not
        // merely emptied. Asserting a count of zero would have passed against a purge,
        // which is a materially different thing to rely on between tests.
        assertThat(output).contains("after reset the queue is gone:");
        assertThat(output).contains("queue 'orders.large' does not exist");

        // And the transport refuses what it cannot honestly provide. A fake that
        // quietly treated a stream as a queue would pass a test and fail in production.
        assertThat(output).contains("streams    refused:");
        assertThat(output).contains("does not support streams");
    }

    private static String runMain() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            TestingWithoutABroker.main(new String[0]);
        } catch (Exception e) {
            System.setOut(original);
            System.out.println(captured.toString(StandardCharsets.UTF_8));
            throw e;
        } finally {
            System.setOut(original);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }
}
