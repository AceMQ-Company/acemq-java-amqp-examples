package org.acemq.examples.advanced;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.crypto.SecretKey;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.Codecs;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.crypto.EncryptedCodec;
import org.acemq.amqp.crypto.Keyring;
import org.acemq.amqp.crypto.Keys;
import org.acemq.amqp.test.InMemoryTransport;
import org.acemq.amqp.transport.QueueType;

/**
 * Making the payload opaque to the broker.
 *
 * <p>TLS protects a message in flight. It does nothing about one sitting in a queue that an
 * operator, a backup or the management UI can read. Where the payload must be opaque to the
 * broker itself, it has to arrive already encrypted — and {@link Codec} is the seam, because
 * it is the last thing to touch the bytes going out and the first coming in.
 *
 * <p>Earlier versions of this example hand-rolled the codec, because the library had none.
 * It now ships one, and the parts that were listed here as "what a real version would need"
 * are the parts worth showing: a key identifier per message, and rotation without a flag day.
 *
 * <p>No Docker needed: {@code mvn compile exec:java}.
 */
public final class EncryptingPayloads {

    public record Payment(String id, String cardHolder, double amount) { }

    public static void main(String[] args) throws Exception {
        InMemoryTransport.reset();

        // In production these come from a key management service and Keyring is a small class
        // in front of it. Generated here because the example has to run.
        SecretKey june = Keys.generate();
        SecretKey september = Keys.generate();

        try (AceMq mq = AceMq.connect("memory://payments")) {
            mq.declareExchange("payments", "topic");
            mq.declareQueue("payments.new", QueueType.CLASSIC, Map.of());
            mq.bind("payments.new", "payments", "payment.*");

            // The delegate does the serialising; this only wraps it. Format and encryption
            // stay independent choices.
            Codec json = Codecs.byName("json");
            Codec encrypting = EncryptedCodec.wrapping(json, Keyring.of("payments-2026-06", june));

            Payment payment = new Payment("p-1", "A. Customer", 42.00);

            // What the broker would hold, either way. The first is readable by anyone with the
            // management UI; the second is not.
            System.out.printf("  plain      %s%n", preview(json.encode(payment)));
            System.out.printf("  encrypted  %s%n", preview(encrypting.encode(payment)));

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

            // The content type describes the wire format, not what is under the encryption. A
            // "+json" suffix would make the JSON codec volunteer to parse ciphertext, so the
            // failure would surface in a parser rather than in a codec that knows it cannot help.
            System.out.printf("  offered to json codec: %s%n",
                    json.canDecode(EncryptedCodec.CONTENT_TYPE));
            System.out.printf("  offered to this codec: %s%n",
                    encrypting.canDecode(EncryptedCodec.CONTENT_TYPE));

            // Rotation. September's service writes with September's key and still holds June's
            // for what is queued. This is why the key identifier is in the message: a consumer
            // reads which key it needs rather than assuming the current one.
            byte[] writtenInJune = encrypting.encode(payment);
            Codec rotated = EncryptedCodec.wrapping(json, Keyring.builder()
                    .add("payments-2026-06", june)
                    .current("payments-2026-09", september)
                    .build());

            System.out.printf("  june message needs key %s, and still reads: %s%n",
                    EncryptedCodec.keyIdOf(writtenInJune),
                    rotated.decode(writtenInJune, Payment.class).id());
            System.out.printf("  new messages are written with %s%n",
                    EncryptedCodec.keyIdOf(rotated.encode(payment)));

            // A retired key is named, along with what is held instead, because the usual cause
            // of an undecryptable queue is a key retired while messages were still in it.
            Codec withoutJune = EncryptedCodec.wrapping(
                    json, Keyring.of("payments-2026-09", september));
            try {
                withoutJune.decode(writtenInJune, Payment.class);
                System.out.println("  retired key unexpectedly decrypted");
            } catch (AceMqException e) {
                System.out.printf("  retired key refused: %s%n", firstSentence(e.getMessage()));
            }
        }
    }

    /** Only for showing what the broker would hold. */
    private static String preview(byte[] body) {
        String text = new String(body, StandardCharsets.UTF_8);
        return text.codePoints().anyMatch(c -> c < 0x20 || c > 0x7e)
                ? "<" + body.length + " bytes of ciphertext>"
                : text;
    }

    private static String firstSentence(String message) {
        int stop = message.indexOf(". ");
        return stop < 0 ? message : message.substring(0, stop + 1);
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
