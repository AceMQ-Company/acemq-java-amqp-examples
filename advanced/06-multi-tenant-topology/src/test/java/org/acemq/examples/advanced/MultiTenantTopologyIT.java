package org.acemq.examples.advanced;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("advanced/06 — multi-tenant topology")
class MultiTenantTopologyIT {

    @Test
    @Timeout(120)
    void oneTenantsFloodDoesNotReachTheOthers() throws Exception {
        String output = runMain();

        assertThat(output).contains("topology   3 queues, one per tenant");

        // The broker did the separation. The consumer never filtered, and could not
        // have forgotten to.
        assertThat(output).contains("acme saw   [a-1]");
        assertThat(output).doesNotContain("acme saw   [a-1, g-1");

        // The blast radius, which is the whole argument for a queue per tenant: 501
        // messages for one customer, one each for the others.
        assertThat(output).containsPattern("globex     queue holds 501");
        assertThat(output).containsPattern("initech    queue holds 1");
    }

    private static String runMain() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            MultiTenantTopology.main(new String[0]);
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
