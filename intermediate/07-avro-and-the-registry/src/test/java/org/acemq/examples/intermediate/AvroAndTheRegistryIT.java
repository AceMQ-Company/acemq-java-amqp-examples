package org.acemq.examples.intermediate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("intermediate/07 — Avro and the schema registry")
class AvroAndTheRegistryIT {

    @Test
    @Timeout(60)
    void theSchemaIdentifierTravelsWithTheMessage() throws Exception {
        String output = runMain();

        assertThat(output).contains("registered id=1 subject=org.acemq.examples.OrderPlaced");

        // The fingerprint is deterministic: the same schema always produces it, which is
        // what lets a registry recognise a schema it already holds instead of issuing a
        // second identifier for it.
        assertThat(output).contains(
                "fingerprint 1a39a1ec78ccebe151264c1c1c7924ee66e562dca24e67ef289dcaefc6e9158d");

        assertThat(output).contains("received   [o-1=42.0]");

        // Seventeen bytes for a record with a three-character id and a double: five of
        // framing and twelve of Avro. The field names live in the schema, not in every
        // copy of the message.
        assertThat(output).contains("on the wire 17 bytes, id=1, content-type=application/vnd.acemq.avro");

        assertThat(output).contains("registered=application/vnd.acemq.avro fixed=avro/binary");

        // The framings are refused in both the places that matter: decoding directly,
        // and choosing a codec by content type. Before 0.2.4 the first returned an empty
        // id and a total of 5.4e-67 without throwing, and the second returned true.
        assertThat(output).contains("mismatch   refused:");
        assertThat(output).contains("would silently produce the wrong values");
        assertThat(output).contains("canDecode  fixed codec on registered messages=false");
    }

    private static String runMain() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            AvroAndTheRegistry.main(new String[0]);
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
