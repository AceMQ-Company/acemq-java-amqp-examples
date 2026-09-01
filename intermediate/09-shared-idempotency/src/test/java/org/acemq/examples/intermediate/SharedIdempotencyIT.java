package org.acemq.examples.intermediate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("intermediate/09 — shared idempotency")
class SharedIdempotencyIT {

    @Test
    @Timeout(60)
    void oneInstanceWinsAndAnAbandonedClaimIsRecoverable() throws Exception {
        String output = runMain();

        // Two instances, one message, one winner. The losing claim returning false is
        // the entire contract.
        assertThat(output).contains("claim      A=true B=false");
        assertThat(output).contains("confirmed  isConfirmed=true");

        // Still refused later, which is what retention buys: a redelivery weeks after
        // both instances have been replaced is still recognised.
        assertThat(output).contains("again      claim=false");

        // The lease. An instance that claims and dies must not make a message
        // permanently unhandleable, so the claim expires and another instance takes it.
        assertThat(output).contains("orphan     B cannot take it yet=true");
        assertThat(output).contains("orphan     after the lease expires, B claims it=true");

        // And the deliberate release, for a handler that failed and wants the message
        // retried now rather than when the lease runs out.
        assertThat(output).contains("released   another instance can claim at once=true");
    }

    private static String runMain() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            SharedIdempotency.main(new String[0]);
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
