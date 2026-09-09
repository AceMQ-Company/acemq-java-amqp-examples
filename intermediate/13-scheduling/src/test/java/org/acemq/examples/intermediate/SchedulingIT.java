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
@DisplayName("intermediate/13 — delivering a message later")
class SchedulingIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(300)
    void delayedMessagesWaitAndAnOverdueOneDoesNot() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        // Everything handed to the ladder came out of it, and the delayed ones really did move
        // between rungs rather than being delivered on the spot.
        assertThat(output).containsPattern("scheduled 3, delivered 3, hops [1-9]\\d*");

        // The content type is carried in a header and put back on the message finally
        // delivered. Without that the bytes arrive intact and undecodable.
        assertThat(output).contains("content type on arrival: application/json");

        double past = arrivedAt(output, "past", "R-0");
        double three = arrivedAt(output, "3s", "R-1");
        double five = arrivedAt(output, "5s", "R-2");

        // Already due, so it waited for nothing.
        assertThat(past).as("a past-dated reminder is late, not an error").isLessThan(1.5);

        // And the delayed ones waited. This is the check worth having: a scheduler that
        // delivered everything at once would satisfy every other assertion here.
        assertThat(three).as("a three-second delay").isGreaterThanOrEqualTo(1.5);
        assertThat(five).as("a five-second delay").isGreaterThanOrEqualTo(3.5);
        assertThat(five).isGreaterThan(three);

        // The trade, asserted rather than described. A message ships as soon as under a second
        // remains, so a three-second delay lands at about two and is not meant to land at three.
        assertThat(three).as("delivery is accurate to about the smallest rung").isLessThan(3.5);
    }

    private static double arrivedAt(String output, String askedFor, String id) {
        Pattern row = Pattern.compile(Pattern.quote(askedFor) + "\\s+([\\d.]+)s\\s+" + Pattern.quote(id));
        Matcher matcher = row.matcher(output);
        assertThat(matcher.find()).as("no row for %s in:%n%s", id, output).isTrue();
        return Double.parseDouble(matcher.group(1));
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            Scheduling.main(new String[] {url});
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
