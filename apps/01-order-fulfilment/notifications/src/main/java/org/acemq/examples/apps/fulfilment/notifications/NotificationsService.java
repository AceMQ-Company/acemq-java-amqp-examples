package org.acemq.examples.apps.fulfilment.notifications;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.Codecs;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.examples.apps.fulfilment.contracts.Fulfilment;

/**
 * Tells the customer what happened.
 *
 * <p>Bound to {@code fulfilment.#} — everything. This is the service that shows why a
 * topic exchange is worth more than a queue per pair of services: it was added without a
 * single change to any publisher, and the next one will be too.
 *
 * <p>It cannot ask for a typed payload, because it subscribes to six different event types
 * on one queue and their shapes differ. So it takes the body as text and reads the
 * envelope, which carries the type and the correlation id -- everything this service
 * actually needs.
 *
 * <p>The {@code text} codec is the part worth copying. Asking for {@code String.class}
 * without it means the JSON codec is handed an object and told to produce a String, which
 * fails with "no String-argument constructor" for every message. That is a fan-in
 * consumer's most common mistake and the failure names the wrong thing.
 */
public final class NotificationsService implements AutoCloseable {

    private final AceMq mq;
    private final MessageConsumer consumer;
    private final Map<String, List<String>> timeline = new ConcurrentHashMap<>();

    public NotificationsService(String amqpUrl, Telemetry telemetry) {
        this.mq = AceMq.connect(amqpUrl, telemetry);
        mq.topology().apply(Fulfilment.topology(), ApplyMode.CREATE_ONLY);

        this.consumer = mq.consume(Fulfilment.NOTIFICATIONS, String.class,
                ConsumerOptions.prefetch(50).as(Codecs.byName("text")),
                message -> {
                    // The correlation id is the order it belongs to, set by whichever
                    // service published it and carried forward by all of them.
                    String order = message.envelope().correlationId();
                    timeline.computeIfAbsent(order, id -> new CopyOnWriteArrayList<>())
                            .add(message.envelope().type());
                });
    }

    /** What a customer looking at "where is my order" would be shown. */
    public List<String> timelineOf(String orderId) {
        return List.copyOf(timeline.getOrDefault(orderId, List.of()));
    }

    public int ordersSeen() {
        return timeline.size();
    }

    @Override
    public void close() {
        consumer.drain(Duration.ofSeconds(10));
        consumer.close();
        mq.close();
    }
}
