package org.acemq.examples.basic;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.security.Credentials;
import org.acemq.amqp.security.Security;
import org.acemq.amqp.transport.ConnectionConfig;

/**
 * Connecting over TLS, with credentials that come from somewhere sensible.
 *
 * <p>Run {@code ./generate-dev-certs.sh} first, then {@code docker compose up -d}, then
 * {@code mvn compile exec:java}.
 */
public final class TlsAndCredentials {

    public record Order(String id, double total) { }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQPS_URL", "amqps://localhost:5671");
        Path certs = Path.of(args.length > 1 ? args[1]
                : System.getenv().getOrDefault("ACEMQ_CERTS", "certs"));

        // The URL decides whether the connection is encrypted; the policy decides how
        // strictly the certificate is checked. Those are separate on purpose -- an
        // amqp:// URL that quietly upgraded itself would be impossible to reason about.
        Security security = Security.fromKeystore(certs)
                // The built-in default is "acemq", which keytool will not accept when
                // creating a keystore -- PKCS12 passwords must be at least six
                // characters. Anything real reads this from a secret store anyway.
                .keystorePassword("acemq-dev")
                // Credentials are asked for on every connection, not read once at
                // start-up. That is what makes rotation work: change the password in
                // your secret store and the next reconnect picks it up, with no
                // redeployment. In a real service this reads from Vault or similar.
                .withCredentials(() -> Credentials.of(
                        System.getenv().getOrDefault("AMQP_USER", "guest"),
                        System.getenv().getOrDefault("AMQP_PASSWORD", "guest")))
                // Needed here and nowhere else. These certificates carry
                // "ACEMQ DEVELOPMENT ONLY - DO NOT TRUST", and AceMQ refuses them
                // unless asked, so one drifting into production fails closed.
                .allowDevelopmentCertificates();

        try (AceMq mq = AceMq.connect(ConnectionConfig.url(url).security(security).build())) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.secure");
            mq.bind("orders.secure", "orders", "order.*");

            CountDownLatch handled = new CountDownLatch(1);
            try (MessageConsumer consumer = mq.consume("orders.secure", Order.class, message -> {
                System.out.printf("  consumed  %s over TLS%n", message.payload().id());
                handled.countDown();
            })) {
                mq.publisher("orders", "order.placed", Order.class).send(new Order("o-2001", 99.50));
                System.out.println("  published over an encrypted connection");
                if (!handled.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("the message was never delivered");
                }
            }
        }

        // Asking for TLS and then asking for no verification is a contradiction, and
        // one of the two was a mistake. AceMQ refuses rather than guessing which.
        try {
            AceMq.connect(ConnectionConfig.url(url).security(Security.disabled()).build()).close();
            System.out.println("  unreachable: amqps:// with security disabled should be refused");
        } catch (AceMqException expected) {
            System.out.printf("  refused   amqps:// with verification switched off (%s: %s)%n",
                    expected.getClass().getSimpleName(), expected.getMessage());
        }

        // And without the explicit opt-in, the development certificate is rejected --
        // which is the whole point of the marker.
        try {
            AceMq.connect(ConnectionConfig.url(url)
                    .security(Security.fromKeystore(certs).keystorePassword("acemq-dev"))
                    .build()).close();
            System.out.println("  unreachable: a development certificate should be refused");
        } catch (Exception expected) {
            Throwable cause = expected;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            System.out.printf("  refused   a development certificate without the opt-in (%s: %s)%n",
                    cause.getClass().getSimpleName(), cause.getMessage());
        }
    }
}
