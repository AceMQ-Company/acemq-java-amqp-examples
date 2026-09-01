package org.acemq.examples.advanced;

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
@DisplayName("advanced/05 — message expiry")
class MessageExpiryIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(240)
    void expiredMessagesAreDeadLetteredAndOnlyLeaveAtTheHead() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        assertThat(output).contains("supported  TTL_PER_MESSAGE=true");

        // Expired, and countable: with a dead-letter exchange they arrive somewhere
        // rather than vanishing, which is what makes "how many did we drop" answerable.
        assertThat(output).contains("after 3s   queued=0 expired=2");

        // The behaviour worth knowing: RabbitMQ expires a message when it reaches the
        // head, not on a timer. An expired message behind a live one is still counted.
        assertThat(output).contains("queued=2 (q-3 has expired but is still counted)");

        // And when the head moves, the expired one is dropped rather than delivered.
        assertThat(output).contains("delivered  [blocker]");
        assertThat(output).contains("after the  blocker is consumed: queued=0 expired=3");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            MessageExpiry.main(new String[] {url});
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
