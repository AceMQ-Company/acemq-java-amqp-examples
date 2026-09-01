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
@DisplayName("basic/05 — idempotent consumer")
class IdempotentConsumerIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(180)
    void theDuplicateIsHandledOnce() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        // Both halves are asserted. Without the first, the example could be
        // suppressing a duplicate that was never delivered twice, and the reader
        // would be shown a guarantee that had not been exercised.
        assertThat(output).contains("no store       card charged 2 times for one order");
        assertThat(output).contains("with a store   card charged 1 time, 1 duplicate suppressed");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            IdempotentConsumer.main(new String[] {url});
        } finally {
            System.setOut(original);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }
}
