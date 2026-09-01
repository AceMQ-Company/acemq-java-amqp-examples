package org.acemq.examples.intermediate;

import java.util.Map;

import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Topology;
import org.acemq.amqp.api.TopologyPlan;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.test.InMemoryTransport;

/**
 * The exchanges, queues and bindings as a value, planned before they are applied.
 *
 * <p>Topology declared by scattered {@code declareQueue} calls is topology nobody can
 * read: what exists depends on which services started, in what order, with which version
 * deployed. Declared as data it can be printed, diffed, reviewed in a pull request, and —
 * the part that matters at three in the morning — <em>planned</em> against what is really
 * there before anything is changed.
 *
 * <p>No Docker needed: {@code mvn compile exec:java}.
 */
public final class TopologyAsData {

    /**
     * One place that says what this service needs. A reviewer can read it; a start-up
     * sequence of declare calls spread across three classes cannot be read at all.
     */
    private static final Topology ORDERS = Topology.define()
            .exchange("orders", "topic")
            .classicQueue("orders.new", Map.of())
            .classicQueue("orders.audit", Map.of())
            .bind("orders.new", "orders", "order.placed")
            .bind("orders.audit", "orders", "order.*")
            .build();

    public static void main(String[] args) throws Exception {
        InMemoryTransport.reset();

        try (AceMq mq = AceMq.connect("memory://orders")) {
            // Plan first. Nothing is created; this asks what would change, which is the
            // question a deployment wants answered before it does anything.
            TopologyPlan planned = mq.topology().plan(ORDERS);
            System.out.printf("  plan       hasChanges=%s%n", planned.hasChanges());
            System.out.print(indent(planned.render()));

            // DRY_RUN goes through the same path and still changes nothing, which is what
            // makes it worth having in a deployment as a check rather than a comment.
            TopologyPlan dry = mq.topology().apply(ORDERS, ApplyMode.DRY_RUN);
            System.out.printf("  dry run    %d actions, still nothing created%n",
                    dry.changes().size());

            TopologyPlan applied = mq.topology().apply(ORDERS, ApplyMode.CREATE_ONLY);
            System.out.printf("  applied    %d created%n", applied.changes().size());

            // Applying the same topology again is safe and is meant to be routine: it
            // is what lets a service declare its own topology on every start-up rather
            // than someone doing it once by hand, which is how environments drift apart.
            //
            // The plan does not come back empty, though, and the reason is worth
            // knowing. Queues are inspected, so they turn into `present`. Exchanges and
            // bindings cannot be: AMQP has no way to ask whether one exists except a
            // passive declare, which kills the channel when the answer is no. Since
            // redeclaring an equivalent exchange or binding is harmless, the planner
            // reports them as creations, because that is honestly what apply will
            // attempt.
            TopologyPlan again = mq.topology().plan(ORDERS);
            System.out.printf("  again      %d of %d still reported as changes%n",
                    again.changes().size(), again.actions().size());
            System.out.print(indent(again.render()));

            mq.publisher("orders", "order.placed", String.class).asText().send("o-1");
            System.out.printf("  routing    orders.new=%d orders.audit=%d%n",
                    mq.messageCount("orders.new"), mq.messageCount("orders.audit"));
        }

        // Somebody adds a queue. The plan says what is new before anything happens, and
        // the queues that already exist say so -- which is the review question answered:
        // "what will this deployment actually change?"
        Topology extended = Topology.define()
                .exchange("orders", "topic")
                .classicQueue("orders.new", Map.of())
                .classicQueue("orders.audit", Map.of())
                .classicQueue("orders.fraud", Map.of())
                .bind("orders.new", "orders", "order.placed")
                .bind("orders.audit", "orders", "order.*")
                .bind("orders.fraud", "orders", "order.placed")
                .build();

        try (AceMq mq = AceMq.connect("memory://orders")) {
            TopologyPlan plan = mq.topology().plan(extended);
            System.out.printf("  next       the only new queue is:%n");
            plan.actions().stream()
                    .filter(action -> action.kind() == TopologyPlan.Kind.CREATE)
                    .filter(action -> action.description().startsWith("queue "))
                    .forEach(action -> System.out.printf("    %s%n", action));
        }
    }

    private static String indent(String rendered) {
        return rendered.lines().map(line -> "    " + line + System.lineSeparator())
                .reduce("", String::concat);
    }
}
