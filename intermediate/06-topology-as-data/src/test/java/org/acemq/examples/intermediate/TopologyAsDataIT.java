package org.acemq.examples.intermediate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("intermediate/06 — topology as data")
class TopologyAsDataIT {

    @Test
    @Timeout(60)
    void aPlanSaysWhatWillChangeBeforeAnythingDoes() throws Exception {
        String output = runMain();

        assertThat(output).contains("plan       hasChanges=true");

        // A dry run computes the same plan and creates nothing. The proof is the line
        // after it: applying still reports five creations, so the dry run really did
        // leave the broker empty.
        assertThat(output).contains("dry run    5 actions, still nothing created");
        assertThat(output).contains("applied    5 created");

        // Re-planning turns the queues into `present` but still reports the exchange
        // and bindings as creations, because AMQP cannot be asked whether either
        // exists without a passive declare that kills the channel when it does not.
        assertThat(output).contains("again      3 of 5 still reported as changes");
        assertThat(output).contains("present  queue orders.new (classic)");
        assertThat(output).contains("present  queue orders.audit (classic)");

        // The topology routes: order.placed reached both the specific binding and the
        // wildcard one.
        assertThat(output).contains("routing    orders.new=1 orders.audit=1");

        // And adding a queue plans exactly one new queue, not three.
        assertThat(output).contains("CREATE queue orders.fraud (classic)");
        assertThat(output).doesNotContain("CREATE queue orders.new");
    }

    private static String runMain() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            TopologyAsData.main(new String[0]);
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
