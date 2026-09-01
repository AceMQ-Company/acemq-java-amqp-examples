package org.acemq.examples.basic;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Runs the TLS example against a RabbitMQ actually serving TLS.
 *
 * <p>This is the test that makes the security documentation worth reading. The certificates
 * are produced by the same {@code generate-dev-certs.sh} a reader runs, and the broker is
 * configured with them, so a mistake in that script — a missing subject alternative name,
 * the wrong keystore password, a certificate the library refuses — fails here rather than
 * on somebody's first afternoon with the library.
 */
@Testcontainers
@DisplayName("basic/02 — TLS and credentials")
class TlsAndCredentialsIT {

    private static final Path CERTS = Path.of("target", "it-certs");

    // A static block rather than @BeforeAll, and the ordering is the whole reason:
    // a static @Container field is initialised — and the files it mounts are read —
    // before any @BeforeAll runs. Generating the certificates there handed the broker
    // paths that did not exist yet, and it failed to boot with nothing pointing at why.
    static {
        try {
            generateCertificates();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Runs the same Maven goal a reader runs, rather than a fixture standing in for it.
     *
     * <p>{@code mvn -Pgencert} is what the README says to type; this invokes the goal it is
     * bound to, so a change that breaks the documented command breaks this test too.
     */
    private static void generateCertificates() throws Exception {
        String mvn = System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("win")
                ? "mvn.cmd" : "mvn";
        Process process = new ProcessBuilder(mvn, "-B",
                "org.acemq:acemq-security-dev:0.2.0:certs",
                "-Dbroker=localhost",
                "-Dpassword=acemq-dev",
                "-Dout=" + CERTS.toAbsolutePath())
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        boolean finished = process.waitFor(120, TimeUnit.SECONDS);
        assertThat(finished).as("the certs goal should finish").isTrue();
        assertThat(process.exitValue()).as("the certs goal said:%n%s", output).isZero();
        assertThat(Files.exists(CERTS.resolve("truststore.p12"))).isTrue();
        assertThat(Files.exists(CERTS.resolve("server.crt"))).isTrue();
    }

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"))
            // The same rabbitmq.conf the docker-compose file uses, so this test proves
            // what a reader actually runs. Testcontainers' withSSL() helper is not used:
            // it sets RABBITMQ_SSL_* environment variables that RabbitMQ 4 rejects
            // outright as deprecated, and the broker refuses to boot.
            // The rabbitmq.conf the generator emitted, so the test runs exactly what a
            // reader gets rather than a hand-written copy that can drift from it.
            .withRabbitMQConfig(MountableFile.forHostPath(CERTS.resolve("rabbitmq.conf")))
            .withCopyFileToContainer(
                    // An explicit mode: the script writes private keys 0600, which is
                    // right on a developer's machine and unreadable to the broker's user
                    // once mounted -- RabbitMQ then fails to boot saying nothing about it.
                    MountableFile.forHostPath(CERTS.resolve("server.key"), 0644),
                    "/etc/rabbitmq/certs/server.key")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(CERTS.resolve("server.crt"), 0644),
                    "/etc/rabbitmq/certs/server.crt")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(CERTS.resolve("ca.crt"), 0644),
                    "/etc/rabbitmq/certs/ca.crt");

    @Test
    @Timeout(180)
    void theExampleRunsOverTls() throws Exception {
        String output = runMain(BROKER.getAmqpsUrl(), CERTS.toString());

        assertThat(output).contains("published over an encrypted connection");
        assertThat(output).contains("consumed  o-2001 over TLS");
        // Both refusals are the point of the example, and both are asserted on the
        // reason rather than on the fact that something threw. A test happy with any
        // exception would pass just as well if the broker were simply unreachable,
        // and would then be certifying a security guarantee that was never exercised.
        assertThat(output)
                .as("the policy should refuse the contradiction itself, not fail to connect")
                .contains("the URL asks for amqps:// and the security policy is disabled");
        assertThat(output)
                .as("the development marker should be what rejects the certificate")
                .contains("this chain contains a development certificate");
        assertThat(output).doesNotContain("unreachable");
    }

    private static String runMain(String url, String certs) throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            TlsAndCredentials.main(new String[] {url, certs});
        } finally {
            System.setOut(original);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }
}
