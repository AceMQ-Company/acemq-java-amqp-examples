package org.acemq.examples.advanced;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("advanced/02 — encrypting payloads")
class EncryptingPayloadsIT {

    @Test
    @Timeout(60)
    void theBrokerHoldsCiphertextAndOnlyTheKeyHolderCanReadIt() throws Exception {
        String output = runMain();

        // What the broker would hold in each case. The plaintext line is the point of
        // comparison: without it, "encrypted" is a claim rather than a demonstration.
        assertThat(output).contains("plain      {\"id\":\"p-1\",\"cardHolder\":\"A. Customer\"");
        assertThat(output).contains("encrypted  <100 bytes of ciphertext>");
        assertThat(output).doesNotContain("encrypted  {");

        // And it still round trips through a real publish and consume.
        assertThat(output).contains("round trip [A. Customer 42.0]");

        // The content type is honest about the wire format, so the JSON codec does not
        // volunteer for bytes it cannot parse. Naming it "...+json" made this true, and
        // a message would then have failed inside a parser rather than being refused by
        // a codec that knew it could not help.
        assertThat(output).contains("offered to json codec: false");
        assertThat(output).contains("offered to this codec: true");

        // GCM authenticates as well as encrypts, so a wrong key and a tampered message
        // are the same answer: neither reaches the application.
        assertThat(output).contains("june message needs key payments-2026-06, and still reads: p-1");
        assertThat(output).contains("new messages are written with payments-2026-09");
        // The identifier a message needs is readable without holding any key, which is what an
        // operator has in front of a dead-letter queue they can no longer read.
        assertThat(output).contains("retired key refused: this message was encrypted with key"
                + " 'payments-2026-06', which is not in the keyring.");
    }

    private static String runMain() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            EncryptingPayloads.main(new String[0]);
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
