package org.acemq.examples.advanced;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.Codecs;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.test.InMemoryTransport;
import org.acemq.amqp.transport.QueueType;

/**
 * Making the payload opaque to the broker.
 *
 * <p>TLS protects a message in flight. It does nothing about one sitting in a queue that an
 * operator, a backup or the management UI can read. The library's own security page says
 * so and tells you to encrypt in your own code — this is that code, and the seam it uses
 * is {@link Codec}, the last thing to touch the bytes going out and the first coming in.
 *
 * <p>No Docker needed: {@code mvn compile exec:java}.
 */
public final class EncryptingPayloads {

    public record Payment(String id, String cardHolder, double amount) { }

    public static void main(String[] args) throws Exception {
        InMemoryTransport.reset();

        // In production this comes from a key management service, and each message carries
        // an identifier saying which key encrypted it, so a key can be rotated without a
        // flag day. Generated here because the example has to run.
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        SecretKey key = generator.generateKey();

        try (AceMq mq = AceMq.connect("memory://payments")) {
            mq.declareExchange("payments", "topic");
            mq.declareQueue("payments.new", QueueType.CLASSIC, Map.of());
            mq.bind("payments.new", "payments", "payment.*");

            // The delegate does the serialising; this codec only wraps it. Format and
            // encryption stay independent choices.
            Codec json = Codecs.byName("json");
            EncryptingCodec encrypting = new EncryptingCodec(json, key);

            Payment payment = new Payment("p-1", "A. Customer", 42.00);

            // What the broker would hold, either way. The first is readable by anyone with
            // the management UI; the second is not.
            System.out.printf("  plain      %s%n", EncryptingCodec.preview(json.encode(payment)));
            System.out.printf("  encrypted  %s%n", EncryptingCodec.preview(encrypting.encode(payment)));

            List<String> received = new CopyOnWriteArrayList<>();
            try (MessageConsumer consumer = mq.consume("payments.new", Payment.class,
                    ConsumerOptions.defaults().as(encrypting),
                    message -> received.add(message.payload().cardHolder()
                            + " " + message.payload().amount()))) {

                mq.publisher("payments", "payment.taken", Payment.class)
                  .as(encrypting)
                  .send(payment);

                waitFor(() -> received.size() == 1, Duration.ofSeconds(10));
                System.out.printf("  round trip %s%n", received);
            }

            // The content type says both what it is and that it is wrapped, so a consumer
            // without the key does not silently receive something it cannot use -- it is
            // never offered the message at all.
            System.out.printf("  offered to json codec: %s%n",
                    json.canDecode(EncryptingCodec.CONTENT_TYPE));
            System.out.printf("  offered to this codec: %s%n",
                    encrypting.canDecode(EncryptingCodec.CONTENT_TYPE));

            // A different key is the same answer as a tampered message, because GCM
            // authenticates as well as encrypts. Neither reaches the application.
            KeyGenerator other = KeyGenerator.getInstance("AES");
            other.init(256);
            try {
                new EncryptingCodec(json, other.generateKey())
                        .decode(encrypting.encode(payment), Payment.class);
                System.out.println("  wrong key  unexpectedly decrypted");
            } catch (AceMqException e) {
                System.out.printf("  wrong key  refused%n");
            }
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier done, Duration limit) throws Exception {
        long deadline = System.nanoTime() + limit.toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("timed out waiting for the example to progress");
            }
            Thread.sleep(20);
        }
    }
}
