package org.acemq.examples.intermediate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("intermediate/10 — graceful shutdown")
class GracefulShutdownIT {

    @Test
    @Timeout(60)
    void drainReportsWhetherTheHandlersActuallyFinished() throws Exception {
        String output = runMain();

        // A grace period longer than the handler: nothing left in flight.
        assertThat(output).contains("enough time drained=true inFlight=0");

        // A grace period shorter than the handler: drain says so. This return value is
        // the signal worth alerting on -- it means work was abandoned mid-flight and
        // will be redelivered to whoever starts next. Asserting only the true case
        // would leave the useful half of the contract untested.
        assertThat(output).contains("not enough  drained=false inFlight=1");

        assertThat(output).contains("no drain    closed without draining");
    }

    private static String runMain() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            GracefulShutdown.main(new String[0]);
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
