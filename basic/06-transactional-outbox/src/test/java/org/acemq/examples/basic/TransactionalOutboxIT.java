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
@DisplayName("basic/06 — transactional outbox")
class TransactionalOutboxIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(180)
    void theRolledBackOrderPublishesNothing() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        assertThat(output).contains("committed  order o-1");
        assertThat(output).contains("rolled back order o-2");

        // One order in the database, and one message published. The rollback is the
        // assertion that matters: if the outbox wrote outside the transaction, o-2
        // would be here too, and the example would be demonstrating the bug it exists
        // to prevent.
        assertThat(output).contains("orders=1");
        // The body arrives exactly as it was stored, which is what makes the
        // transactional write safe: the outbox holds the wire form, not an object
        // that has to be re-serialised later by a process that may not exist.
        assertThat(output).contains("published  [o-1 o-1 42.0]");
        // Specifically that no o-2 payload was published. "o-2 " alone matches the
        // rolled-back line in the output, which is not the same claim at all.
        assertThat(output).doesNotContain("o-2 {");
        assertThat(output).contains("outbox now pending=0");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            TransactionalOutbox.main(new String[] {url});
        } catch (Exception e) {
            // Whatever the example managed to print before it failed is the most
            // useful thing here; without it the failure is just a stack trace with
            // no idea how far it got.
            System.setOut(original);
            System.out.println(captured.toString(StandardCharsets.UTF_8));
            throw e;
        } finally {
            System.setOut(original);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }
}
