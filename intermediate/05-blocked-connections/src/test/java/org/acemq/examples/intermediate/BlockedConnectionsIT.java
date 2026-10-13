package org.acemq.examples.intermediate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("intermediate/05 — blocked connections")
class BlockedConnectionsIT {

    @Test
    @Timeout(30)
    void aBlockedBrokerRefusesPublishesAndSaysWhy() throws Exception {
        String output = runMain();

        assertThat(output).contains("before     published, blocked=false");
        assertThat(output).contains("alarm      raised, isBlocked=true reason=low on disk");

        // The reason is the broker's own words. That is what belongs in an alert:
        // "low on disk" is actionable, "publish failed" is not.
        assertThat(output).contains("during     refused: reason=low on disk");

        // Whether the message may have reached the broker decides whether a retry needs
        // an idempotency key. Rejected before it left, so this one is safe to resend.
        assertThat(output).contains("during     mayHaveBeenPublished=false");

        // With the default blockedTimeout of zero the publish is refused at once and
        // holds the caller's thread for no time at all, because that number is what
        // decides whether an alarm degrades a service or stops it. (Before 0.7.15 the
        // default was a 30s wait.)
        assertThat(output).contains("during     the publish waited about 0 s");

        // Messages published before the alarm are untouched, and the queue is readable
        // throughout -- an alarm stops publishers so consumers can drain.
        assertThat(output).contains("meanwhile  the queue still holds 1");

        assertThat(output).contains("after      cleared, published again, blocked=false");
        assertThat(output).contains("queue      holds 2");
    }

    private static String runMain() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            BlockedConnections.main(new String[0]);
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
