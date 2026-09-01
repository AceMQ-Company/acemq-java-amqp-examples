package org.acemq.examples.advanced;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("advanced/03 — dead-letter triage")
class DeadLetterTriageIT {

    @Test
    @Timeout(120)
    void onlyTheFixedCauseIsReplayed() throws Exception {
        String output = runMain();

        assertThat(output).contains("dead       4 in orders.new.dlq");

        // Two moved, two left. The count is the whole point: replaying all four would
        // put the messages that cannot succeed straight back through the ladder.
        assertThat(output).contains("replayed   2,");
        assertThat(output).contains("handled    [o-1, o-2]");
        assertThat(output).contains("remaining  2 for a person to look at");

        // A payload that will never decode goes to the parking lot, not the
        // dead-letter queue, so a bulk replay never picks it up.
        assertThat(output).contains("parked     1 that will never decode");
    }

    private static String runMain() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            DeadLetterTriage.main(new String[0]);
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
