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
@DisplayName("basic/03 — retries and dead letters")
class RetriesAndDeadLettersIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(240)
    void theExampleRetriesThenGivesUp() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        // Recovered on a later attempt: the retry actually happened rather than the
        // first delivery quietly succeeding.
        assertThat(output).contains("recovered");
        assertThat(output).contains("retried");

        assertThat(output).contains("gave up");
        assertThat(output).contains("parked");

        // The fatal case must not spend the whole ladder. Asserting the wording keeps
        // the example honest about the difference it is demonstrating.
        assertThat(output).contains("refused    a hopeless message after 1 attempt, not 3");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            RetriesAndDeadLetters.main(new String[] {url});
        } finally {
            System.setOut(original);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }
}
