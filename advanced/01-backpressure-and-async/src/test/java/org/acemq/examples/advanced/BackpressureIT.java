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

@Testcontainers
@DisplayName("advanced/01 — backpressure and async publishing")
class BackpressureIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(300)
    void pipeliningIsFasterAndStillConfirmsEverything() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        // The claim that matters more than the speed: all three thousand were
        // confirmed. A faster publish that dropped messages would be worthless.
        assertThat(output).contains("queued      3000 messages, all confirmed");

        long oneByOne = milliseconds(output, "send        1000 messages in\\s+(\\d+) ms");
        long all = milliseconds(output, "sendAll     1000 messages in\\s+(\\d+) ms");
        long async = milliseconds(output, "sendAsync   1000 messages in\\s+(\\d+) ms");

        // A thousand round trips cannot be instant. If this ever passes at single-digit
        // milliseconds, confirms have quietly stopped being awaited.
        assertThat(oneByOne)
                .as("a confirm per message is a round trip per message")
                .isGreaterThan(100);

        // Locally this is 20-30x. Asserting 3x leaves room for a loaded CI machine while
        // still failing if pipelining regresses to waiting per message.
        assertThat((double) oneByOne / all).as("sendAll vs send").isGreaterThan(3.0);
        assertThat((double) oneByOne / async).as("sendAsync vs send").isGreaterThan(3.0);
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
            Backpressure.main(new String[] {url});
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
