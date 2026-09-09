package org.acemq.examples.advanced;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.Codecs;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.patterns.ClaimCheckCodec;
import org.acemq.amqp.patterns.FilesystemClaimCheckStore;

/**
 * Keeping a large payload off the broker, and leaving a small one alone.
 *
 * <p>A scanned report is tens of megabytes. Putting it on a queue is possible and is a mistake:
 * it sits in the broker's memory, it is copied to every bound queue, it makes a dead-letter queue
 * impossible to look at by hand, and it turns a broker into a filesystem with worse tools. What
 * travels instead is a <strong>claim check</strong> — the payload goes to a store, and the
 * message carries the key.
 *
 * <p>The part that is easy to get wrong is the other half. Offloading a two-hundred-byte message
 * turns one broker round trip into a store round trip <em>and</em> a broker round trip, so an
 * unconditional claim check makes the common case slower in order to fix the rare one. Below the
 * threshold the payload travels inline, exactly as it would without this codec, and the three
 * bytes on the front say which of the two it is so a consumer handles both without being told.
 *
 * <p>This run sends two documents that differ by <strong>one byte</strong>, either side of the
 * threshold. That boundary is the whole point of the codec and it is invisible in a run where
 * every message is large.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class ClaimCheck {

    public record Document(String id, String body) { }

    private static final String EXCHANGE = "documents";

    /** Read with the claim-check codec, so both shapes arrive as a Document. */
    private static final String STORED = "documents.stored";

    /** The same messages read as raw bytes, which is what the broker was actually holding. */
    private static final String RAW = "documents.raw";

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        Path store = Files.createTempDirectory("acemq-claim-check");
        try (AceMq mq = AceMq.connect(url)) {
            mq.declareExchange(EXCHANGE, "topic");
            mq.declareQueue(STORED);
            mq.declareQueue(RAW);
            mq.bind(STORED, EXCHANGE, "document.*");
            mq.bind(RAW, EXCHANGE, "document.*");

            // The delegate does the serialising; this only decides where the bytes end up. A
            // claim-checked document is still a document, so the content type stays the
            // delegate's -- unlike encryption, where the bytes really are something else.
            Codec json = Codecs.byName("json");
            Codec checked = ClaimCheckCodec.wrapping(json, new FilesystemClaimCheckStore(store));

            // One byte apart, either side of 65536. The comparison is "smaller than the
            // threshold travels inline", so a payload of exactly 65536 is offloaded and a
            // payload of 65535 is not.
            Document justUnder = ofEncodedSize(json, "doc-just-under", ClaimCheckCodec.DEFAULT_THRESHOLD - 1);
            Document exactly = ofEncodedSize(json, "doc-exactly", ClaimCheckCodec.DEFAULT_THRESHOLD);

            System.out.printf("  threshold  %d bytes, and the payload is measured after it is serialised%n",
                    ClaimCheckCodec.DEFAULT_THRESHOLD);
            System.out.printf("  store      %s%n", store);
            System.out.println();

            List<byte[]> onTheWire = new CopyOnWriteArrayList<>();
            List<Document> delivered = new CopyOnWriteArrayList<>();

            try (MessageConsumer raw = mq.consume(RAW, byte[].class,
                            ConsumerOptions.defaults().as(Codecs.byName("bytes")),
                            message -> onTheWire.add(message.payload()));
                    MessageConsumer reader = mq.consume(STORED, Document.class,
                            ConsumerOptions.defaults().as(checked),
                            message -> delivered.add(message.payload()))) {

                send(mq, checked, justUnder, onTheWire, 1);
                send(mq, checked, exactly, onTheWire, 2);

                System.out.printf("  %-16s %10s %12s   %-8s  %s%n",
                        "document", "serialised", "on the wire", "framing", "claim check");
                describe(justUnder, json, onTheWire.get(0));
                describe(exactly, json, onTheWire.get(1));

                // The headline. One byte more in the document, and 65 kilobytes less on the
                // broker -- because the payload is now in the store and what travelled is a key.
                require(ClaimCheckCodec.keyOf(onTheWire.get(0)) == null,
                        "a payload of " + (ClaimCheckCodec.DEFAULT_THRESHOLD - 1)
                                + " bytes should have travelled inline");
                String key = ClaimCheckCodec.keyOf(onTheWire.get(1));
                require(key != null,
                        "a payload of exactly " + ClaimCheckCodec.DEFAULT_THRESHOLD
                                + " bytes should have been offloaded, and travelled inline");
                require(onTheWire.get(1).length < 100,
                        "the offloaded message should have been a key, and was "
                                + onTheWire.get(1).length + " bytes");

                System.out.println();
                System.out.printf("  in the store  %s is %d bytes, and the broker never saw them%n",
                        key, Files.size(store.resolve(key)));
                require(Files.size(store.resolve(key)) == ClaimCheckCodec.DEFAULT_THRESHOLD,
                        "the store should hold the whole serialised document");

                // Both arrive as a Document. The consumer was not told which of them was
                // offloaded, and there is nothing in its code that could act on the answer.
                waitFor(() -> delivered.size() == 2, Duration.ofSeconds(30));
                delivered.sort(Comparator.comparing(Document::id));
                System.out.printf("  round trip    %s%n", delivered.stream()
                        .map(document -> document.id() + " " + document.body().length() + " chars")
                        .toList());
                require(delivered.stream().anyMatch(justUnder::equals) && delivered.stream().anyMatch(exactly::equals),
                        "both documents should have come back exactly as they were sent");

                // A message written before this codec existed, or by a publisher that does not
                // use it. Reading it as the delegate would is what makes adding a claim check to
                // a live queue an ordinary release rather than a flag day.
                Document old = new Document("doc-written-earlier", "written before the codec existed");
                mq.publisher(EXCHANGE, "document.stored", Document.class).as(json).send(old);
                waitFor(() -> delivered.size() == 3, Duration.ofSeconds(30));
                System.out.printf("  unframed      a message with no framing still reads: %s%n",
                        delivered.stream().filter(old::equals).count() == 1);
                require(delivered.contains(old),
                        "a message written without this codec should still be readable with it");

                // The retention rule, as a failure rather than as advice. The store has to
                // outlast every queue the message can reach, every dead-letter queue behind
                // them, and any replay somebody does by hand.
                new FilesystemClaimCheckStore(store).delete(key);
                try {
                    checked.decode(onTheWire.get(1), Document.class);
                    throw new IllegalStateException("a message whose payload has been deleted should not decode");
                } catch (AceMqException expected) {
                    System.out.printf("  deleted       %s%n", firstSentence(expected.getMessage()));
                }
            }
        } finally {
            deleteRecursively(store);
        }
    }

    /** Publishes, and waits for the bytes to have been seen on the wire. */
    private static void send(AceMq mq, Codec checked, Document document, List<byte[]> onTheWire, int expected)
            throws Exception {
        mq.publisher(EXCHANGE, "document.stored", Document.class).as(checked).send(document);
        waitFor(() -> onTheWire.size() == expected, Duration.ofSeconds(30));
    }

    private static void describe(Document document, Codec json, byte[] body) {
        String key = ClaimCheckCodec.keyOf(body);
        System.out.printf("  %-16s %10d %12d   %-8s  %s%n",
                document.id(), json.encode(document).length, body.length, framing(body),
                key == null ? "-" : key);
    }

    /** The three bytes on the front: 0xAC 0x01 0x00 inline, 0xAC 0x01 0x01 a claim check. */
    private static String framing(byte[] body) {
        return String.format("%02X %02X %02X", body[0] & 0xFF, body[1] & 0xFF, body[2] & 0xFF);
    }

    /**
     * A document whose serialised form is exactly this many bytes.
     *
     * <p>The threshold is compared against what the delegate produced, not against the object, so
     * a document either side of it has to be built by measuring rather than by guessing.
     */
    private static Document ofEncodedSize(Codec json, String id, int bytes) {
        int overhead = json.encode(new Document(id, "")).length;
        return new Document(id, "a".repeat(bytes - overhead));
    }

    private static String firstSentence(String message) {
        int stop = message.indexOf(". ");
        return stop < 0 ? message : message.substring(0, stop + 1);
    }

    private static void deleteRecursively(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> entries = Files.walk(directory)) {
            entries.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                    // A temporary directory that outlives the example is not worth a failure.
                }
            });
        }
    }

    /** Fails the example rather than printing something untrue. */
    private static void require(boolean claim, String whatWasExpected) {
        if (!claim) {
            throw new IllegalStateException(whatWasExpected);
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
