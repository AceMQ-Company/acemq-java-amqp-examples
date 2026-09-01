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
@DisplayName("basic/07 — serialization formats")
class SerializationFormatsIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    @Test
    @Timeout(180)
    void oneConsumerReadsEveryFormat() throws Exception {
        String output = runMain(BROKER.getAmqpUrl());

        // All three, through a consumer that was never told which format to expect.
        assertThat(output).contains("json=1.0", "xml=2.0", "yaml=3.0");
        // And the publishers really are different objects writing different formats,
        // which is the claim asXml() makes.
        assertThat(output).contains("json=application/json");
        assertThat(output).contains("xml=application/xml");
    }

    private static String runMain(String url) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            SerializationFormats.main(new String[] {url});
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
