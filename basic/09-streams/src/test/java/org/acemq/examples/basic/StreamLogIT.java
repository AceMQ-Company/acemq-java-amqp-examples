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
@DisplayName("basic/09 — streams")
class StreamLogIT {

    // Plain RabbitMQ, no plugins. Stream queues are part of the broker; the
    // rabbitmq_stream plugin only serves the separate stream protocol, which this
    // library does not use. If that were wrong this test would fail at declareStream.
    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(240)
    void aStoppedReaderResumesWhereItStoppedAndTheLogSurvivesBeingRead() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        assertThat(output).contains("wrote      10 orders");
        assertThat(output).contains("read       5, then the reader stopped");

        // The checkpoint is the offset of the last message handled, not the count.
        assertThat(output).contains("checkpoint offset 4");

        // Resuming from checkpoint + 1 picks up exactly what the first reader did not
        // handle: no gap, and nothing handled twice.
        assertThat(output).contains("resumed    [o-5, o-6, o-7, o-8, o-9]");

        // And the point of a stream rather than a queue: two readers before it, and a
        // third still sees the whole log.
        assertThat(output).contains("a new reader still saw all 10");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            StreamLog.main(new String[] {url});
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
