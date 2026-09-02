package org.acemq.examples.apps.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.examples.apps.ledger.contracts.Ledger;
import org.acemq.examples.apps.ledger.ledger.LedgerModule;
import org.acemq.examples.apps.ledger.projections.StatementProjection;
import org.acemq.examples.apps.ledger.transfers.TransferGateway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * The ledger, against a real broker with real streams.
 *
 * <p>The tests that matter here are the last two: a projection rebuilt from nothing that agrees
 * with the writer, and a second projection started later that still sees all of history. Those
 * are the claims event sourcing makes, and they are either true or the architecture is a story.
 */
@Testcontainers
@DisplayName("an event-sourced ledger")
class LedgerIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:4-management"));

    private AceMq mq;
    private LedgerModule ledger;
    private TransferGateway transfers;

    @BeforeEach
    void startTheLedger() {
        mq = AceMq.connect(BROKER.getAmqpUrl(), Telemetry.NONE);
        mq.topology().apply(Ledger.topology(), ApplyMode.CREATE_ONLY);

        ledger = new LedgerModule(mq);
        transfers = new TransferGateway(mq);
    }

    @AfterEach
    void stopTheLedger() {
        for (AutoCloseable module : new AutoCloseable[] {transfers, ledger}) {
            try {
                if (module != null) {
                    module.close();
                }
            } catch (Exception ignored) {
                // Shutting down is not the subject of this test.
            }
        }
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("a transfer posts two entries that sum to zero")
    void doubleEntry() {
        ledger.fund("alice", 10_000);
        waitFor(() -> ledger.balanceOf("alice") == 10_000);

        transfers.request("alice", "bob", 2_500, "rent");

        waitFor(() -> ledger.balanceOf("bob") == 2_500);
        assertThat(ledger.balanceOf("alice")).isEqualTo(7_500);

        // The invariant: money is neither created nor destroyed by a transfer.
        assertThat(ledger.balanceOf("alice") + ledger.balanceOf("bob")).isEqualTo(10_000);
    }

    @Test
    @Timeout(180)
    @DisplayName("a transfer that would overdraw is refused, and the refusal is recorded")
    void insufficientFunds() {
        ledger.fund("carol", 1_000);
        waitFor(() -> ledger.balanceOf("carol") == 1_000);

        transfers.request("carol", "dave", 5_000, "optimistic");

        waitFor(() -> !transfers.refused().isEmpty());
        assertThat(transfers.refused().get(0).reason()).contains("insufficient funds");

        // Nothing was posted. A ledger that half-applies a refused transfer is worse than one
        // that refuses loudly.
        assertThat(ledger.balanceOf("carol")).isEqualTo(1_000);
        assertThat(ledger.balanceOf("dave")).isZero();
    }

    @Test
    @Timeout(180)
    @DisplayName("a projection built from offset zero agrees with the writer")
    void projectionAgreesWithTheWriter() throws Exception {
        ledger.fund("erin", 20_000);
        waitFor(() -> ledger.balanceOf("erin") == 20_000);
        transfers.request("erin", "frank", 3_000, "invoice 1");
        transfers.request("erin", "frank", 4_000, "invoice 2");
        waitFor(() -> ledger.balanceOf("frank") == 7_000);

        // A reader that has never seen a message before, starting from the beginning of time.
        // It stores nothing the log does not contain, and it must reach the same answer.
        try (StatementProjection statements = new StatementProjection(mq, true)) {
            waitFor(() -> statements.balanceOf("frank") == 7_000);

            assertThat(statements.balanceOf("erin")).isEqualTo(ledger.balanceOf("erin"));
            assertThat(statements.balanceOf("frank")).isEqualTo(ledger.balanceOf("frank"));

            // And it has the detail the balance does not: three entries against erin — the
            // opening balance and two debits.
            assertThat(statements.statementOf("erin")).hasSize(3);
            assertThat(statements.statementOf("frank"))
                    .extracting(Ledger.EntryPosted::description)
                    .containsExactly("invoice 1", "invoice 2");
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("a projection added later still gets all of history")
    void aLaterProjectionSeesEverything() throws Exception {
        ledger.fund("grace", 5_000);
        transfers.request("grace", "heidi", 1_000, "before the projection existed");
        waitFor(() -> ledger.balanceOf("heidi") == 1_000);

        // Started now, after the entries were written. On a queue there would be nothing left
        // to read -- the ledger's own reader consumed it. A stream is not emptied by reading,
        // so this gets everything, and so would one written next year.
        try (StatementProjection late = new StatementProjection(mq, true)) {
            waitFor(() -> late.balanceOf("heidi") == 1_000);

            assertThat(late.statementOf("heidi")).hasSize(1);
            assertThat(late.statementOf("heidi").get(0).description())
                    .isEqualTo("before the projection existed");
        }
    }

    @Test
    @Timeout(180)
    @DisplayName("two readers of the same stream do not compete for entries")
    void readersDoNotCompete() throws Exception {
        ledger.fund("ivan", 8_000);
        transfers.request("ivan", "judy", 2_000, "shared");
        waitFor(() -> ledger.balanceOf("judy") == 2_000);

        try (StatementProjection first = new StatementProjection(mq, true);
                StatementProjection second = new StatementProjection(mq, true)) {

            waitFor(() -> first.balanceOf("judy") == 2_000 && second.balanceOf("judy") == 2_000);

            // Both saw the same entry. On a queue exactly one of them would have, which is the
            // property that makes a queue wrong for a ledger and right for a command.
            assertThat(first.statementOf("judy")).hasSize(1);
            assertThat(second.statementOf("judy")).hasSize(1);
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier done) {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the ledger did not reach the expected state in time");
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted", e);
            }
        }
    }
}
