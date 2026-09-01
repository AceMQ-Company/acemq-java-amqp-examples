package org.acemq.examples.intermediate;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.SchemaDefinition;
import org.acemq.amqp.codec.avro.AvroCodec;
import org.acemq.amqp.codec.avro.InMemorySchemaRegistry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.test.InMemoryTransport;
import org.acemq.amqp.transport.QueueType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Avro on the wire, and the schema identifier that makes it readable later.
 *
 * <p>Avro bytes carry no description of themselves. Given a payload and no schema there is
 * nothing to do with it — which is the trade for how small it is. A registry closes that
 * gap: the writer puts a schema id on the front of every message, and a reader resolves
 * the id to the schema that wrote it.
 *
 * <p>No Docker needed: {@code mvn compile exec:java}.
 */
public final class AvroAndTheRegistry {

    private static final Schema ORDER = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "OrderPlaced",
              "namespace": "org.acemq.examples",
              "fields": [
                {"name": "id", "type": "string"},
                {"name": "total", "type": "double"}
              ]
            }
            """);

    public static void main(String[] args) throws Exception {
        InMemoryTransport.reset();

        // A registry maps an integer to a schema. In production this is Confluent's, or
        // Apicurio, or a table in your own database -- the interface is two methods, and
        // this one is a HashMap. What matters is that both ends agree on the mapping.
        InMemorySchemaRegistry registry = new InMemorySchemaRegistry();
        SchemaDefinition definition = AvroCodec.definitionOf(ORDER);
        registry.register(1, definition);
        System.out.printf("  registered id=1 subject=%s%n", definition.subject());

        // The fingerprint is what makes "is this the same schema" answerable without
        // comparing two blobs of JSON, and what a registry uses to avoid handing out a
        // second id for a schema it already knows.
        System.out.printf("  fingerprint %s%n", definition.fingerprint());

        AvroCodec codec = AvroCodec.registered(registry);

        try (AceMq mq = AceMq.connect("memory://orders")) {
            mq.declareExchange("orders", "topic");
            mq.declareQueue("orders.new", QueueType.CLASSIC, Map.of());
            mq.bind("orders.new", "orders", "order.placed");

            List<String> received = new CopyOnWriteArrayList<>();
            try (MessageConsumer consumer = mq.consume("orders.new", GenericRecord.class,
                    ConsumerOptions.defaults().as(codec),
                    message -> received.add(message.payload().get("id")
                            + "=" + message.payload().get("total")))) {

                GenericRecord order = new GenericData.Record(ORDER);
                order.put("id", "o-1");
                order.put("total", 42.0);

                mq.publisher("orders", "order.placed", GenericRecord.class)
                  .as(codec)
                  .send(order);

                waitFor(() -> received.size() == 1, Duration.ofSeconds(10));
                System.out.printf("  received   %s%n", received);
            }
        }

        // What is actually on the wire: a magic byte, four bytes of schema id, then the
        // Avro body. Five bytes buys the reader the ability to know what it is holding.
        byte[] encoded = codec.encode(order("o-2", 7.5));
        System.out.printf("  on the wire %d bytes, id=%d, content-type=%s%n",
                encoded.length, idOf(encoded), codec.contentType());

        System.out.printf("  framings   registered=%s fixed=%s%n",
                AvroCodec.registered(registry).contentType(), AvroCodec.of(ORDER).contentType());

        // The two framings are not interchangeable, and the difference is invisible in
        // the bytes: a fixed-schema codec starts reading at offset zero, so the five
        // bytes of framing become the beginning of the first field. Avro raises nothing
        // -- it decodes whatever the shifted bytes happen to mean -- so before 0.2.4
        // this returned an empty id and a total of 5.4e-67, reported as success. Writing
        // this example is how that was found.
        try {
            AvroCodec.of(ORDER).decode(encoded, GenericRecord.class);
            System.out.println("  mismatch   unexpectedly decoded");
        } catch (AceMqException e) {
            System.out.printf("  mismatch   refused: %s%n", firstSentence(e.getMessage()));
        }

        // The same refusal on the path a consumer actually takes, where the codec is
        // chosen by content type before anything is decoded.
        System.out.printf("  canDecode  fixed codec on registered messages=%s%n",
                AvroCodec.of(ORDER).canDecode(AvroCodec.registered(registry).contentType()));
    }

    private static String firstSentence(String message) {
        int stop = message.indexOf(". ");
        return stop < 0 ? message : message.substring(0, stop);
    }

    private static GenericRecord order(String id, double total) {
        GenericRecord record = new GenericData.Record(ORDER);
        record.put("id", id);
        record.put("total", total);
        return record;
    }

    /** The four bytes after the magic byte. */
    private static int idOf(byte[] encoded) {
        return (encoded[1] & 0xFF) << 24 | (encoded[2] & 0xFF) << 16
                | (encoded[3] & 0xFF) << 8 | encoded[4] & 0xFF;
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
