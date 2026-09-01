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
@DisplayName("basic/10 — pipelines")
class OrderPipelineIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(240)
    void eightOrdersReachDispatchAndTheFilteredOneStopsAtValidate() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        assertThat(output).contains("steps      [validate, price, dispatch]");

        // One queue per step, which is what makes a backlog name the slow step.
        assertThat(output).contains("fulfilment.validate", "fulfilment.price", "fulfilment.dispatch");

        assertThat(output).contains(
                "dispatched [TRK-o-1, TRK-o-2, TRK-o-3, TRK-o-4, TRK-o-5, TRK-o-6, TRK-o-7, TRK-o-8]");

        // The filtered order stopped at the first step. Asserting the tracking list
        // above alone would not show that: an order that failed at dispatch would look
        // the same from there.
        assertThat(output).contains("rejected   [o-free] at validate");
        assertThat(output).doesNotContain("TRK-o-free");

        // Ended early is counted apart from completed. A filter recorded as a failure
        // would put every rejected order in a dead-letter queue somebody has to read.
        assertThat(output).contains("counts     entered=9 completed=8 endedEarly=1");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            OrderPipeline.main(new String[] {url});
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
