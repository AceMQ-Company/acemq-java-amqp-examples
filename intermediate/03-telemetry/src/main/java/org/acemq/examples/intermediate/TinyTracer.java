package org.acemq.examples.intermediate;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Telemetry;

/**
 * A tracer small enough to read, which is the point.
 *
 * <p>Real deployments implement {@link Telemetry} against OpenTelemetry, and the interface
 * is shaped for it: the same three ideas — a scope that ends, an outcome, and headers to
 * carry the context across the broker. Seventy lines here rather than a dependency, so
 * that what the library asks of a tracer is visible.
 */
final class TinyTracer implements Telemetry {

    /** One recorded span: what happened, in which trace, under which parent. */
    record Span(String name, String traceId, String spanId, String parentId, String outcome) {

        @Override
        public String toString() {
            return name + " outcome=" + outcome;
        }
    }

    private final List<Span> finished = new CopyOnWriteArrayList<>();

    /**
     * The span currently open on this thread.
     *
     * <p>A thread local is what makes a publish inside a consume handler a child of that
     * handler rather than a new trace. Every tracer does some version of this; a real one
     * uses OpenTelemetry's Context, which also survives thread hops.
     */
    private final ThreadLocal<Span> current = new ThreadLocal<>();

    List<Span> finished() {
        return List.copyOf(finished);
    }

    @Override
    public Scope publishStarted(String exchange, String routingKey, Envelope envelope) {
        return start("publish " + routingKey, envelope);
    }

    @Override
    public Scope consumeStarted(String queue, Envelope envelope) {
        return start("consume " + queue, envelope);
    }

    private Scope start(String name, Envelope envelope) {
        Span parent = current.get();
        String traceId;
        String parentId;
        if (parent != null) {
            // Inside another operation on this thread: same trace.
            traceId = parent.traceId();
            parentId = parent.spanId();
        } else {
            // Nothing open here, so the trace has to come from the message if it carries
            // one. This is the hop across the broker: the consumer is a different thread
            // in a different process, and the header is the only link back.
            String traceparent = String.valueOf(envelope.headers().get("traceparent"));
            String[] parts = traceparent.split("-");
            if (parts.length == 4) {
                traceId = parts[1];
                parentId = parts[2];
            } else {
                traceId = id(32);
                parentId = "";
            }
        }

        Span span = new Span(name, traceId, id(16), parentId, "unset");
        current.set(span);
        return new Scope() {
            private String outcome = "unset";

            @Override
            public void outcome(String value) {
                outcome = value;
            }

            @Override
            public void failed(Throwable failure) {
                outcome = "failed";
            }

            @Override
            public void close() {
                finished.add(new Span(span.name(), span.traceId(), span.spanId(),
                        span.parentId(), outcome));
                current.remove();
            }
        };
    }

    /**
     * The headers that carry the trace to whoever handles the message next.
     *
     * <p>Called while the publish scope is open, so it describes the span being created.
     * That ordering is the whole contract: gather the context before the scope opens and
     * every message is stamped with its caller's span instead of its own.
     */
    @Override
    public Map<String, String> propagationHeaders() {
        Span span = current.get();
        if (span == null) {
            return Map.of();
        }
        return Map.of("traceparent", "00-" + span.traceId() + "-" + span.spanId() + "-01");
    }

    @Override
    public void messageRetried(String queue, Envelope envelope, Duration delay) {
        finished.add(new Span("retry " + queue, traceOf(envelope), id(16), "", "retried"));
    }

    @Override
    public void messageDeadLettered(String queue, Envelope envelope, String reason) {
        finished.add(new Span("dead-letter " + queue, traceOf(envelope), id(16), "", reason));
    }

    private static String traceOf(Envelope envelope) {
        String traceparent = String.valueOf(envelope.headers().get("traceparent"));
        String[] parts = traceparent.split("-");
        return parts.length == 4 ? parts[1] : "";
    }

    private static String id(int length) {
        StringBuilder builder = new StringBuilder(length);
        while (builder.length() < length) {
            builder.append(Integer.toHexString(ThreadLocalRandom.current().nextInt(16)));
        }
        return builder.toString();
    }
}
