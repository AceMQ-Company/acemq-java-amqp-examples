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
@DisplayName("intermediate/12 — saga")
class OrderSagaIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(300)
    void compensationsRunBackwardsAndTheOneThatFailedIsNamed() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        // The happy path, for contrast: three steps, three events, nothing undone.
        assertThat(output).contains("run 1      complete=true steps=[reserve-stock, take-payment, book-courier]");
        assertThat(output).contains("the broker saw: [stock.reserved, payment.taken, courier.booked]");

        // A saga reports rather than raises, because the caller has to decide what happens next
        // and the interesting part is not the exception.
        assertThat(output).contains("run 2      complete=false compensated=true failedAt=book-courier");
        assertThat(output).contains("because no courier will collect from that postcode");

        // Newest first. Releasing the stock before refunding the payment would be undoing them
        // in the order they were done, which is the order in which they depend on each other.
        assertThat(output).contains(
                "the broker saw: [stock.reserved, payment.taken, payment.refunded, stock.released]");

        // The run that matters. A compensation itself fails, and what comes back names the step
        // nobody undid rather than throwing it into a message handler where it would be retried,
        // dead-lettered and forgotten.
        assertThat(output).contains("run 3      unresolved=[reserve-stock]");
        assertThat(output).contains("UNRESOLVED [reserve-stock]");

        // And the refund still ran. A failed compensation does not stop the ones after it:
        // stopping leaves more undone than continuing.
        assertThat(output).contains("the broker saw: [stock.reserved, payment.taken, payment.refunded]");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            OrderSaga.main(new String[] {url});
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
