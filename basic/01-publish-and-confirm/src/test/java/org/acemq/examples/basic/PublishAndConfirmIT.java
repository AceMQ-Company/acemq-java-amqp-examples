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

/**
 * Runs the example against a real broker and checks what it printed.
 *
 * <p>Every example has one of these, and it is the whole reason this repository can be
 * trusted. Examples rot: the library moves, the example does not, and the first thing a
 * newcomer meets is a build error. A test that runs the actual {@code main} method turns
 * that into a red build here instead.
 */
@Testcontainers
@DisplayName("basic/01 — publish and confirm")
class PublishAndConfirmIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(120)
    void theExampleRunsAndDoesWhatItSays() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        assertThat(output).contains("published");
        assertThat(output).contains("consumed  o-1001 for acme");
        // The unroutable publish must be refused rather than silently dropped: that is
        // the behaviour the example exists to demonstrate.
        assertThat(output).contains("refused");
        assertThat(output).doesNotContain("unreachable");
    }

    /** Runs the example's main method, returning everything it printed. */
    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            PublishAndConfirm.main(new String[] {url});
        } finally {
            System.setOut(original);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }
}
