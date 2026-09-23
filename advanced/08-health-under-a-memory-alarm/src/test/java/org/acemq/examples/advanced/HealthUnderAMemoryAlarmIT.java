package org.acemq.examples.advanced;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the example against a real broker and checks what it printed.
 *
 * <p>The broker is this test's own, started by Testcontainers and thrown away afterwards,
 * which is what makes running this alongside everything else safe: the memory alarm the
 * example raises belongs to a node, and every publisher on that node is refused while it
 * lasts. On a shared broker this example would fail whichever other example happened to be
 * publishing, and would look innocent doing it.
 */
@Testcontainers
@DisplayName("advanced/08 — health under a memory alarm")
class HealthUnderAMemoryAlarmIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(180)
    void theExampleRunsAndDoesWhatItSays() throws Exception {
        String output = runMain();

        // Before the alarm: an ordinary connection, and an ordinary probe.
        assertThat(output).contains("before     200 ");
        assertThat(output).contains("\"blocked\":false");

        // The alarm is the broker's, not the client's. If this line is missing, the
        // watermark was set and RabbitMQ did not act on it, and nothing below was measured
        // against a block at all.
        assertThat(output).contains("blocked    the broker said so:");
        assertThat(output).containsPattern("blocked    the broker said so: .*memory");

        // Read from state the connection already holds, so no round trip and no waiting.
        // Asserted as well as printed because the number is the whole behaviour: a check
        // that asks the broker first does not come back until the alarm clears.
        assertThat(microsecondsToReadTheFacts(output))
                .as("the health facts are answered without asking the broker anything")
                .isLessThan(100_000);

        // UP on 200 while blocked, which is what keeps an orchestrator from restarting a
        // healthy process into the same blocked broker. Every one of these is a separate
        // claim the documentation makes, so each is checked rather than summarised.
        assertThat(output).contains("during     200 ");
        assertThat(output).contains("\"status\":\"UP\"");
        assertThat(output).contains("\"blocked\":true");
        assertThat(output).containsPattern("\"blockedReason\":\"[^\"]*memory[^\"]*\"");

        // Publishing is what an alarm actually stops, and it is refused with the broker's
        // own reason rather than swallowed.
        assertThat(output).containsPattern("publish    refused after 2 s: reason=.*memory");
        assertThat(output).contains("mayHaveBeenPublished=false");

        // The control. Without it everything above would pass just as happily against a
        // broker that was never blocked, and would be asserting that a fast thing is fast.
        assertThat(output).contains("control    a queue lookup on this same connection has returned after 5s: false");

        // And the broker is usable afterwards: the watermark is back and the connection is
        // no longer blocked, which is also why closing it did not hang.
        assertThat(output).contains("restored   the watermark is back at 0.4");
        assertThat(output).contains("after      200 ");
        assertThat(output).containsPattern("after      200 .*\"blocked\":false");
    }

    private static int microsecondsToReadTheFacts(String output) {
        Matcher matcher = Pattern.compile("facts {6}read in (\\d+) us").matcher(output);
        assertThat(matcher.find()).as("the example should report how long the facts took: %s", output).isTrue();
        return Integer.parseInt(matcher.group(1));
    }

    /** Runs the example's main method, returning everything it printed. */
    private static String runMain() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            // rabbitmqctl lives inside the broker's container, and the container is this
            // test's own, so the command is built from the id Testcontainers gave it.
            HealthUnderAMemoryAlarm.main(new String[] {
                BROKER.getAmqpUrl(), "docker", "exec", BROKER.getContainerId(), "rabbitmqctl"
            });
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
