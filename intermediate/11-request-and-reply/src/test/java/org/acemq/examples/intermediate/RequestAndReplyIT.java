package org.acemq.examples.intermediate;

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
@DisplayName("intermediate/11 — request and reply")
class RequestAndReplyIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(300)
    void answersFindTheirCallerAndBothReplyAddressesAreHonoured() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        // The round trip at its simplest.
        assertThat(output).contains("asked      SKU-0 x4 -> 1000 pence");

        // Three questions on one reply queue, answered in a different order from the one they
        // were asked in. SKU-1 takes half a second to price, so it comes back last; that the
        // caller still gets its own answer is the correlation id doing its job.
        assertThat(output).contains("asked      [SKU-1, SKU-2, SKU-3]");
        assertThat(output).containsPattern("answered   \\[SKU-[23], SKU-[23], SKU-1\\]");
        assertThat(output).contains("matched    every answer reached the caller that asked for it");

        // The interoperability claim. A request naming only the header is what Go, Python and
        // Ruby send, and answering it is what makes request/reply work across the family.
        assertThat(output).contains("foreign    a caller naming only the acemq-reply-to header was answered");

        // A publish where a request was meant: handled once, counted, not retried forever.
        assertThat(output).contains("unanswerable 1");

        // A timeout is its own outcome and says so. The number after it is what a service would
        // graph to find out that its timeout is too short.
        assertThat(output).containsPattern("timed out  after PT0\\.75S");
        assertThat(output).contains("counters   timedOut=1");

        // The two addresses, side by side. A requester writes the same queue in both places; a
        // caller from another language writes only the header; an ordinary publish writes
        // neither, which is why it cannot be answered.
        assertThat(output).containsPattern("mq\\.requester\\(\\) +acemq\\.reply\\.\\S+ +acemq\\.reply\\.\\S+");
        assertThat(output).containsPattern("another language +<none> +pricing\\.replies\\.foreign");
        assertThat(output).containsPattern("an ordinary publish +<none> +<none>");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            RequestAndReply.main(new String[] {url});
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
