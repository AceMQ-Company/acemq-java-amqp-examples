package org.acemq.examples.intermediate;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.acemq.amqp.api.AceHeaders;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.core.RequestTimedOutException;
import org.acemq.amqp.core.Requester;
import org.acemq.amqp.core.Responder;

/**
 * Asking a question over the broker and waiting for the answer.
 *
 * <p>Before reaching for this: request/reply over a broker is synchronous calling in
 * asynchronous clothes. Where two services can speak HTTP or gRPC they should, because those
 * have timeouts, load balancing and circuit breakers this will not match. Where the callee is
 * reachable only on the broker — behind a firewall, or one worker among many where the broker is
 * already doing the load balancing — this is the right tool, and doing it by hand means reply
 * queues, correlation ids and a timeout somebody forgets.
 *
 * <p>The part worth reading twice is the address. A request names its reply queue <em>twice</em>,
 * in AMQP's own {@code reply-to} property and in the {@code acemq-reply-to} header, always with
 * the same value; a responder reads the header first and the property second. That is what makes
 * a Java requester answerable by a Go responder and the other way round, and it is why the table
 * this prints has two columns.
 *
 * <p>{@code docker compose up -d} then {@code mvn compile exec:java}.
 */
public final class RequestAndReply {

    public record Quote(String sku, int quantity) { }

    public record Price(String sku, long pence) { }

    /** How the request arrived: the AMQP property, and the header. */
    private record Addressed(String property, String header) { }

    private static final String EXCHANGE = "pricing";
    private static final String REQUESTS = "pricing.requests";
    private static final String AUDIT = "pricing.audit";
    private static final String FOREIGN_REPLIES = "pricing.replies.foreign";

    /** Declared, bound to nothing, consumed by nobody. The timeout needs somewhere to go. */
    private static final String UNSERVED = "pricing.unserved";

    private static final String NONE = "<none>";

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0]
                : System.getenv().getOrDefault("AMQP_URL", "amqp://localhost");

        try (AceMq mq = AceMq.connect(url)) {
            mq.declareExchange(EXCHANGE, "topic");
            mq.declareQueue(REQUESTS);
            mq.declareQueue(AUDIT);
            mq.declareQueue(FOREIGN_REPLIES);
            mq.declareQueue(UNSERVED);
            mq.bind(REQUESTS, EXCHANGE, "quote.*");
            mq.bind(AUDIT, EXCHANGE, "quote.*");

            // A second queue on the same binding, so every request can be looked at exactly as
            // it went past. Nothing in production needs this; it is here because the two reply
            // addresses are the point of the example and they are invisible otherwise.
            Map<String, Addressed> onTheWire = new ConcurrentHashMap<>();
            try (MessageConsumer ignored = mq.consume(AUDIT, Quote.class, message -> onTheWire.put(
                    message.payload().sku(),
                    new Addressed(
                            message.replyTo().orElse(NONE),
                            String.valueOf(message.envelope().headers().getOrDefault(AceHeaders.REPLY_TO, NONE)))))) {

                // Pricing takes half a second for SKU-1 and no time at all for the others. The
                // delay is deliberate: it is what makes the answers come back in a different
                // order from the questions, which is the only way to see that they are matched
                // rather than paired off by arrival.
                Function<Quote, Price> pricing = quote -> {
                    if ("SKU-1".equals(quote.sku())) {
                        sleep(Duration.ofMillis(500));
                    }
                    return new Price(quote.sku(), 250L * quote.quantity());
                };

                // Three of them on one queue, each allowed one message at a time. Prefetch
                // matters here: at the default, the first responder to ask would be handed all
                // three requests and price them one after another.
                try (Responder first = mq.respond(REQUESTS, Quote.class, ConsumerOptions.prefetch(1), pricing);
                        Responder second = mq.respond(REQUESTS, Quote.class, ConsumerOptions.prefetch(1), pricing);
                        Responder third = mq.respond(REQUESTS, Quote.class, ConsumerOptions.prefetch(1), pricing);
                        Requester requester = mq.requester()) {

                    oneQuestion(requester);
                    threeAtOnce(requester);
                    aCallerFromAnotherLanguage(mq);
                    aMessageNobodyCanAnswer(mq, List.of(first, second, third));
                    nobodyAnswering(requester);

                    waitFor(() -> onTheWire.containsKey("SKU-0")
                            && onTheWire.containsKey("SKU-9")
                            && onTheWire.containsKey("SKU-X"), Duration.ofSeconds(30));
                    printAddresses(requester, onTheWire);
                }
            }
        }
    }

    /** The whole of the usual case. */
    private static void oneQuestion(Requester requester) {
        Price price = requester.request(
                EXCHANGE, "quote.asked", new Quote("SKU-0", 4), Price.class, Duration.ofSeconds(10));

        System.out.printf("  asked      SKU-0 x4 -> %d pence%n", price.pence());
        require(price.pence() == 1000, "SKU-0 x4 should have been priced at 1000 pence, was " + price.pence());
    }

    /**
     * Three questions in flight on one reply queue.
     *
     * <p>A requester owns a single reply queue for its whole life, so every answer to every
     * caller arrives on it. What sends each one to the right caller is the correlation id, and
     * nothing else — not the order, which is why SKU-1 is made slow.
     */
    private static void threeAtOnce(Requester requester) throws Exception {
        List<String> answered = new CopyOnWriteArrayList<>();
        Map<String, CompletableFuture<Price>> asked = new LinkedHashMap<>();

        for (String sku : List.of("SKU-1", "SKU-2", "SKU-3")) {
            CompletableFuture<Price> reply =
                    requester.requestAsync(EXCHANGE, "quote.asked", new Quote(sku, 1), Price.class);
            reply.thenAccept(price -> answered.add(price.sku()));
            asked.put(sku, reply);
        }

        for (Map.Entry<String, CompletableFuture<Price>> question : asked.entrySet()) {
            Price answer = question.getValue().get(30, TimeUnit.SECONDS);
            require(answer.sku().equals(question.getKey()),
                    "the answer to " + question.getKey() + " carried " + answer.sku()
                            + ", so a reply reached the wrong caller");
        }
        waitFor(() -> answered.size() == 3, Duration.ofSeconds(10));

        System.out.printf("  asked      %s, and SKU-1 takes half a second to price%n", asked.keySet());
        System.out.printf("  answered   %s%n", answered);
        System.out.println("  matched    every answer reached the caller that asked for it");

        // The claim the printout makes. Half a second is a long time for a broker round trip,
        // so this says the answers really did come back out of order rather than that the run
        // happened to be fast.
        require("SKU-1".equals(answered.get(answered.size() - 1)),
                "SKU-1 was the slowest to price and should have been answered last, but the order was "
                        + answered);
    }

    /**
     * A request written the way Go, Python and Ruby write one.
     *
     * <p>Those three name the reply queue in the {@code acemq-reply-to} header and nowhere else.
     * Until the header was read here as well as written, a request like this one reached a Java
     * responder, was decoded, was priced — and the answer went nowhere.
     */
    private static void aCallerFromAnotherLanguage(AceMq mq) throws Exception {
        String correlationId = UUID.randomUUID().toString();
        CompletableFuture<Price> answer = new CompletableFuture<>();

        try (MessageConsumer ignored = mq.consume(FOREIGN_REPLIES, Price.class, message -> {
            if (correlationId.equals(message.envelope().correlationId())) {
                answer.complete(message.payload());
            }
        })) {
            mq.publisher(EXCHANGE, "quote.asked", Quote.class)
                    .send(new Quote("SKU-9", 2), Envelope.of("Quote")
                            .correlationId(correlationId)
                            .header(AceHeaders.REPLY_TO, FOREIGN_REPLIES)
                            .build());

            Price priced = answer.get(30, TimeUnit.SECONDS);
            System.out.printf("  foreign    a caller naming only the %s header was answered: %d pence%n",
                    AceHeaders.REPLY_TO, priced.pence());
            require(priced.sku().equals("SKU-9"), "the foreign caller was answered about " + priced.sku());
        }
    }

    /**
     * A message that named no reply address at all.
     *
     * <p>Almost always a publish where a request was meant. Failing the handler would retry it,
     * then dead-letter it, and produce a queue of messages whose only problem is that nobody
     * asked for anything; counting it says so once and moves on.
     */
    private static void aMessageNobodyCanAnswer(AceMq mq, List<Responder> responders) throws Exception {
        mq.publisher(EXCHANGE, "quote.asked", Quote.class).send(new Quote("SKU-X", 1));

        waitFor(() -> unanswerable(responders) == 1, Duration.ofSeconds(30));
        System.out.printf("  unanswerable %d — handled, counted, and not retried forever%n",
                unanswerable(responders));
    }

    /** Nobody is serving the queue, so the only thing that happens is the wait. */
    private static void nobodyAnswering(Requester requester) {
        try {
            requester.request("", UNSERVED, new Quote("SKU-7", 1), Price.class, Duration.ofMillis(750));
            throw new IllegalStateException(
                    "a request to a queue nobody serves should have timed out, and did not");
        } catch (RequestTimedOutException expected) {
            System.out.printf("  timed out  after %s%n", expected.waited());
            System.out.println("             a timeout does not mean the work did not happen: the request"
                    + " may be queued, running, or done with the reply lost");
        }

        System.out.printf("  counters   timedOut=%d unmatched=%d%n",
                requester.timedOut(), requester.unmatched());
        require(requester.timedOut() == 1, "one request should have given up, timedOut=" + requester.timedOut());
    }

    private static void printAddresses(Requester requester, Map<String, Addressed> onTheWire) {
        Addressed fromRequester = onTheWire.get("SKU-0");
        Addressed fromForeign = onTheWire.get("SKU-9");
        Addressed fromPublish = onTheWire.get("SKU-X");

        System.out.println();
        System.out.printf("  %-24s %-48s %s%n", "the request came from", "reply-to property", "acemq-reply-to header");
        System.out.printf("  %-24s %-48s %s%n", "mq.requester()", fromRequester.property(), fromRequester.header());
        System.out.printf("  %-24s %-48s %s%n", "another language", fromForeign.property(), fromForeign.header());
        System.out.printf("  %-24s %-48s %s%n", "an ordinary publish", fromPublish.property(), fromPublish.header());

        require(fromRequester.property().equals(fromRequester.header()),
                "a requester must name one queue in both places, and named '" + fromRequester.property()
                        + "' and '" + fromRequester.header() + "'");
        require(fromRequester.property().equals(requester.replyQueue()),
                "the address on the wire should be this requester's reply queue");
        require(NONE.equals(fromForeign.property()) && FOREIGN_REPLIES.equals(fromForeign.header()),
                "the foreign caller should have named the header and nothing else");
    }

    private static long unanswerable(List<Responder> responders) {
        return responders.stream().mapToLong(Responder::unanswerable).sum();
    }

    /** Fails the example rather than printing something untrue. */
    private static void require(boolean claim, String whatWasExpected) {
        if (!claim) {
            throw new IllegalStateException(whatWasExpected);
        }
    }

    private static void sleep(Duration howLong) {
        try {
            Thread.sleep(howLong.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while pricing", e);
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
