package org.acemq.examples.apps.policy.documents;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.core.AceMq;
import org.acemq.examples.apps.policy.contracts.Policies;

/**
 * Documents: the claim-check pattern.
 *
 * <p>A medical report scanned at 300 dpi is tens of megabytes. Putting it on a queue is
 * possible and is a mistake — it fills the broker's memory, it is copied to every bound queue,
 * it makes a dead-letter queue impossible to inspect, and it turns a broker into a filesystem
 * with worse tools. What travels instead is a **claim check**: the document goes to a store,
 * and the message carries the key it was stored under.
 *
 * <pre>{@code
 * String key = documents.store(policyId, "medical-report", scanBytes);
 * // the event carries `key`, not `scanBytes`
 * }</pre>
 *
 * <p>The store here is a {@link ConcurrentHashMap}, because the example must run without
 * infrastructure. A real one is S3, Azure Blob Storage or a filesystem — the pattern is
 * identical and the only thing that changes is two method bodies.
 *
 * <p><strong>This belongs in the library, and does not live there yet.</strong> The same was
 * true of the encrypting codec in {@code advanced/02} until it shipped as
 * {@code acemq-amqp-crypto}, and this example will move to a {@code ClaimCheckStore} the same
 * way. Written here first deliberately: the shape of a pattern is easier to get right once
 * something real has needed it, which is the opposite of how saga and claim-check ended up
 * advertised in a README for months without existing.
 *
 * <h2>Retention is the part people forget</h2>
 *
 * <p>The store and the queue have different lifetimes. A message that is replayed a month
 * later carries a key, and if the store expired that key at thirty days the replay produces a
 * message nobody can read — worse than a lost message, because it looks like a message. Store
 * retention must exceed every retention that could bring a message back, and that includes
 * dead-letter queues and manual replay.
 */
public final class DocumentModule {

    private final AceMq mq;
    private final Map<String, byte[]> store = new ConcurrentHashMap<>();

    public DocumentModule(AceMq mq) {
        this.mq = mq;
    }

    /**
     * Stores a document and announces that it exists.
     *
     * @param policyId the policy it belongs to
     * @param kind what sort of document it is
     * @param content the bytes, which do not go anywhere near the broker
     * @return the key the event will carry
     */
    public String store(String policyId, String kind, byte[] content) {
        String key = "doc/" + policyId + "/" + kind + "/" + UUID.randomUUID().toString().substring(0, 8);
        store.put(key, content);

        // The event is a few hundred bytes whatever the document weighs.
        mq.publisher(Policies.EXCHANGE, Policies.DOCUMENT_STORED, Policies.DocumentStored.class)
                .send(new Policies.DocumentStored(policyId, key, kind, content.length),
                        Envelope.of("DocumentStored").correlationId(policyId).build());
        return key;
    }

    /**
     * Redeems a claim check.
     *
     * @param key what the message carried
     * @return the document, when the store still has it
     */
    public Optional<byte[]> fetch(String key) {
        return Optional.ofNullable(store.get(key));
    }

    /** @return how many documents are held */
    public int held() {
        return store.size();
    }
}
