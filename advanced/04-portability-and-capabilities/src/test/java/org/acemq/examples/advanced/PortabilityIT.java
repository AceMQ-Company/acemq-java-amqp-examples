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
@DisplayName("advanced/04 — portability and capabilities")
class PortabilityIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(240)
    void theSameCodeTakesDifferentPathsOnDifferentTransports() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        // Both transports examined in one run, which is what makes the comparison a
        // demonstration rather than an assertion.
        assertThat(output).contains("=== memory://orders ===");
        assertThat(output).contains("supports   4 of 13 capabilities");
        assertThat(output).contains("supports   10 of 13 capabilities");

        // The same declare call took two different paths, decided by what the transport
        // reported rather than by which transport it is.
        assertThat(output).contains("queue      classic — this transport has no quorum queues");
        assertThat(output).contains("queue      quorum, replicated");

        // And an unsupported feature is refused at declare time rather than quietly
        // substituted with something that behaves differently.
        assertThat(output).contains("streams    refused, and says which transport and why");
        assertThat(output).contains("streams    available");

        // Neither transport claims everything. A capability list that was always full
        // would mean nothing.
        // TRANSACTIONS is in this list on purpose. The broker has transactions; the
        // library exposes no way to use them, and a capability describes what the
        // library can do. It was claimed until 0.2.6, which made supports(...) true
        // with nothing to call -- exactly the check this example teaches.
        assertThat(output).contains("missing    [CONSISTENT_HASH_ROUTING, DELAYED_DELIVERY, TRANSACTIONS]");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            Portability.main(new String[] {url});
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
