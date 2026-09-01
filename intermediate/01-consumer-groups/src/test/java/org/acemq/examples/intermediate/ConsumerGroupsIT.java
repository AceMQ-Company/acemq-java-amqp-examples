package org.acemq.examples.intermediate;

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

@Testcontainers
@DisplayName("intermediate/01 — consumer groups")
class ConsumerGroupsIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(300)
    void scalingTheGroupSpreadsTheWorkAndNothingIsLostOnTheWayOut() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        assertThat(output).contains("group      size=1 prefetch=1");
        assertThat(output).contains("scaled to  size=8");

        // Every message handled exactly once across both batches. This is the claim
        // that matters: scaling a live group must not drop or duplicate anything.
        assertThat(output).contains("handled    80 of 80, each exactly once");

        // Draining left nothing in flight, so closing acknowledged everything it had
        // taken. Messages in flight at close are redelivered, which is correct and
        // surprising, and is what drain() exists to avoid.
        assertThat(output).contains("drained    true, inFlight=0");
        assertThat(output).contains("rejected=0");

        // One handler at a time before scaling, several after. Concurrency is measured
        // by counting handlers running at the same moment, not by counting threads:
        // the group dispatches onto a pool, so even the serial run touches a dozen
        // thread names and a thread count proves nothing.
        assertThat(output).containsPattern("1 consumer  40 messages in \\d+ ms, peak concurrency 1");
        assertThat(output).containsPattern("8 consumers 40 messages in \\d+ ms, peak concurrency [2-8]");

        // The handler sleeps 50ms, so a serial batch cannot take less than two seconds
        // and an eight-way one should be near a quarter of that. Asserting 2x rather
        // than 8x leaves room for a loaded CI machine without letting a regression to
        // serial processing pass.
        long serial = milliseconds(output, "1 consumer  40 messages in (\\d+) ms");
        long parallel = milliseconds(output, "8 consumers 40 messages in (\\d+) ms");
        assertThat(serial)
                .as("40 messages, 50ms each, one at a time")
                .isGreaterThanOrEqualTo(2_000);
        assertThat((double) serial / parallel)
                .as("eight consumers should be several times faster, not marginally")
                .isGreaterThan(2.0);
    }

    private static long milliseconds(String output, String regex) {
        Matcher matcher = Pattern.compile(regex).matcher(output);
        assertThat(matcher.find()).as("no match for %s in:%n%s", regex, output).isTrue();
        return Long.parseLong(matcher.group(1));
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            ConsumerGroups.main(new String[] {url});
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
