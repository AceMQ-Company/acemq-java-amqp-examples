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
@DisplayName("intermediate/03 — telemetry and tracing")
class TracingIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(240)
    void allFourOperationsBelongToOneTrace() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        assertThat(output).contains("spans      4");

        // The claim worth testing. Four operations across two broker hops and two
        // consumer threads, and a single trace id -- the difference between a graph
        // that explains a latency spike and four graphs that do not.
        assertThat(output).contains("traces     1");
        assertThat(output).contains("one trace  true");

        assertThat(output).contains("publish order.placed", "consume orders.placed",
                "publish order.shipped", "consume orders.shipped");

        // Exactly one root. If the trace context were gathered before the publish scope
        // opened, or dropped at the broker hop, the consumer spans would be roots too
        // and this would be 3 or 4.
        assertThat(output.lines().filter(line -> line.contains("parent=(root)")).count())
                .as("one root span, the original publish")
                .isEqualTo(1);
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            Tracing.main(new String[] {url});
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
