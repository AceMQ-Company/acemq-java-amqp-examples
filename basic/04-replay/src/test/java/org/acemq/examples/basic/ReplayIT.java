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
@DisplayName("basic/04 — replay")
class ReplayIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(300)
    void theExampleReplaysWhatWasDeadLettered() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        assertThat(output).contains("dead-lettered  3 orders");
        assertThat(output).contains("waiting        3 in orders.new.dlq");

        // The bounded batch is the point of the trial: one, not all three.
        assertThat(output).contains("replayed       1 as a trial, 1 handled");
        assertThat(output).contains("3 handled in total");

        // Provenance is what tells a handler this message has been round the loop.
        // Asserting the values rather than their presence: replayCount=1 and a reset
        // attempt are the two things a reader is being shown.
        assertThat(output).contains("replayedFrom=orders.new.dlq");
        assertThat(output).contains("replayCount=1");
        assertThat(output).contains("attempt=1");

        // The error that caused the dead-lettering survives the round trip.
        assertThat(output).contains("the inventory service is down");

        assertThat(output).contains("dlq now        0");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            Replay.main(new String[] {url});
        } finally {
            System.setOut(original);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }
}
