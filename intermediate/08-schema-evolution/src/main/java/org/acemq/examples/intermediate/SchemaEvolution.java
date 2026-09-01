package org.acemq.examples.intermediate;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.codec.avro.AvroCodec;
import org.acemq.amqp.codec.avro.InMemorySchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Adding a field to a message that thousands of services already read.
 *
 * <p>The question is never "can we add the field". It is "which of the twenty services that
 * read this message do we have to deploy, and in what order". The answer Avro gives, when
 * the field carries a default, is: none of them, in any order.
 *
 * <p>No Docker needed: {@code mvn compile exec:java}.
 */
public final class SchemaEvolution {

    /** What every producer and consumer agreed on last year. */
    private static final Schema V1 = new Schema.Parser().parse("""
            {"type":"record","name":"OrderPlaced","namespace":"org.acemq.examples","fields":[
              {"name":"id","type":"string"},
              {"name":"total","type":"int"}
            ]}
            """);

    /** The same record with a field added — and a default, which is the whole trick. */
    private static final Schema V2 = new Schema.Parser().parse("""
            {"type":"record","name":"OrderPlaced","namespace":"org.acemq.examples","fields":[
              {"name":"id","type":"string"},
              {"name":"total","type":"int"},
              {"name":"currency","type":"string","default":"GBP"}
            ]}
            """);

    /** The same addition done wrong: a new field with no default. */
    private static final Schema V2_NO_DEFAULT = new Schema.Parser().parse("""
            {"type":"record","name":"OrderPlaced","namespace":"org.acemq.examples","fields":[
              {"name":"id","type":"string"},
              {"name":"total","type":"int"},
              {"name":"currency","type":"string"}
            ]}
            """);

    public static void main(String[] args) {
        InMemorySchemaRegistry registry = new InMemorySchemaRegistry()
                .register(1, AvroCodec.definitionOf(V1))
                .register(2, AvroCodec.definitionOf(V2))
                .register(3, AvroCodec.definitionOf(V2_NO_DEFAULT));

        // Producers write whatever version they are on. Nothing here is told about the
        // readers.
        byte[] fromOldProducer = AvroCodec.registered(registry).encode(order(V1, "o-1", 100));
        byte[] fromNewProducer = AvroCodec.registered(registry).encode(order(V2, "o-2", 200));

        // Consumers say which schema they were written against. That second argument is
        // what makes the resolution happen; without it a reader adopts the writer's
        // schema and sees whatever the producer happened to send.
        AvroCodec oldConsumer = AvroCodec.registered(registry, V1);
        AvroCodec newConsumer = AvroCodec.registered(registry, V2);

        // Producer deployed first. The old consumer has never heard of `currency`, and
        // the field is skipped rather than shifting every byte after it.
        GenericRecord seenByOld = oldConsumer.decode(fromNewProducer, GenericRecord.class);
        System.out.printf("  old reader, new message   id=%s total=%s currency=%s%n",
                seenByOld.get("id"), seenByOld.get("total"),
                seenByOld.getSchema().getField("currency") == null ? "(not in my schema)" : "?");

        // Consumer deployed first. The producer has not started sending `currency`, and
        // the reader's default fills it in -- so the new code can be written as though
        // the field were always there.
        GenericRecord seenByNew = newConsumer.decode(fromOldProducer, GenericRecord.class);
        System.out.printf("  new reader, old message   id=%s total=%s currency=%s%n",
                seenByNew.get("id"), seenByNew.get("total"), seenByNew.get("currency"));

        // And both on the same version, for completeness.
        GenericRecord matched = newConsumer.decode(fromNewProducer, GenericRecord.class);
        System.out.printf("  new reader, new message   id=%s total=%s currency=%s%n",
                matched.get("id"), matched.get("total"), matched.get("currency"));

        // The rule this all rests on. Add the same field without a default and there is
        // nothing Avro can put there when an old producer omits it, so the read fails --
        // which in a deployment means every consumer breaking the moment it is rolled out
        // ahead of the producers.
        try {
            AvroCodec.registered(registry, V2_NO_DEFAULT).decode(fromOldProducer, GenericRecord.class);
            System.out.println("  no default                unexpectedly decoded");
        } catch (AceMqException e) {
            System.out.printf("  no default                refused, as it must%n");
        }
    }

    private static GenericRecord order(Schema schema, String id, int total) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("total", total);
        if (schema.getField("currency") != null) {
            record.put("currency", "EUR");
        }
        return record;
    }
}
