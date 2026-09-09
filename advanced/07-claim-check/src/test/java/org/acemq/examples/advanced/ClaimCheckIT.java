package org.acemq.examples.advanced;

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
@DisplayName("advanced/07 — claim check")
class ClaimCheckIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(300)
    void oneByteEitherSideOfTheThresholdTravelsTwoDifferentWays() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        assertThat(output).contains("threshold  65536 bytes");

        // The boundary, which is the reason both documents are here. 65535 bytes travel inline
        // and cost three bytes of framing; 65536 bytes travel as a key. The comparison is
        // strictly-less-than, so a payload of exactly the threshold is the one that is offloaded
        // — an off-by-one here would move a 40MB payload onto the broker and nothing would say so.
        assertThat(output).containsPattern("doc-just-under\\s+65535\\s+65538\\s+AC 01 00\\s+-");
        assertThat(output).containsPattern("doc-exactly\\s+65536\\s+39\\s+AC 01 01\\s+[0-9a-f-]{36}");

        // What the store holds is what the broker did not.
        assertThat(output).containsPattern("in the store  [0-9a-f-]{36} is 65536 bytes");

        // Both come back as a Document. The consumer was never told which of them was offloaded.
        assertThat(output).contains("doc-exactly 65506 chars");
        assertThat(output).contains("doc-just-under 65502 chars");

        // A message written by a publisher that does not use this codec is still readable, which
        // is what makes introducing it on a live queue an ordinary release.
        assertThat(output).contains("unframed      a message with no framing still reads: true");

        // The retention rule, as a failure rather than as advice.
        assertThat(output).contains("deleted       the claim check");
        assertThat(output).contains("is not in the store, so this message cannot be read.");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            ClaimCheck.main(new String[] {url});
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
