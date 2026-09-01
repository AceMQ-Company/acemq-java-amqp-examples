package org.acemq.examples.basic;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@DisplayName("basic/08 — ordered per key")
class OrderedPerKeyIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(240)
    void everyAccountIsAppliedInOrder() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        // Every account in sequence. This is the guarantee; a single "in order: true"
        // would not prove it held for the accounts that were competing.
        assertThat(output).contains("acct-a", "acct-b", "acct-c");
        assertThat(output).doesNotContain("in order: false");
        assertThat(output).contains("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");

        // And more than one thread did the work: ordering achieved by processing
        // everything serially would satisfy the assertion above and defeat the point.
        assertThat(output).containsPattern("handled by [2-9] threads");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            OrderedPerKey.main(new String[] {url});
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
