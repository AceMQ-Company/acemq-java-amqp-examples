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
@DisplayName("intermediate/02 — interceptors")
class InterceptorsIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(240)
    void everyMessageIsStampedAndEveryFailureIsSeen() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        assertThat(output).contains("handled    1, failed 1");

        // The second publish interceptor sees the header the first one added, which is
        // what order() buys: the ordering is the number, not the registration order.
        assertThat(output).contains("audit:tenant=acme");

        // And the consumer received it, though no publisher call site mentions a tenant.
        assertThat(output).contains("received:tenant=acme");

        // onError fires with the handler's own exception, before the failure policy runs.
        assertThat(output).contains("failed:negative total");

        assertThat(output).contains("confirmed:routed=true");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            Interceptors.main(new String[] {url});
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
