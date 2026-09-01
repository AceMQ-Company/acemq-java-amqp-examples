package org.acemq.examples.intermediate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("intermediate/08 — schema evolution")
class SchemaEvolutionIT {

    @Test
    @Timeout(60)
    void neitherSideHasToBeDeployedFirst() throws Exception {
        String output = runMain();

        // Producer ahead of consumer: the unknown field is skipped, and the fields the
        // consumer does know are still correct -- which is the point. A shifted read
        // would return plausible nonsense instead.
        assertThat(output).contains("old reader, new message   id=o-2 total=200 currency=(not in my schema)");

        // Consumer ahead of producer: the reader's default fills the gap, so new code
        // can be written as though the field were always there.
        assertThat(output).contains("new reader, old message   id=o-1 total=100 currency=GBP");

        // And when both are on the new version the real value arrives, not the default.
        // Without this line the previous assertion would also pass if the codec always
        // returned the default and ignored the message.
        assertThat(output).contains("new reader, new message   id=o-2 total=200 currency=EUR");

        // The rule the whole thing rests on.
        assertThat(output).contains("no default                refused, as it must");
    }

    private static String runMain() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            SchemaEvolution.main(new String[0]);
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
