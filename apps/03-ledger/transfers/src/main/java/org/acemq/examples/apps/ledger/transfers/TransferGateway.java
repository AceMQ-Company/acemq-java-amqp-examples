package org.acemq.examples.apps.ledger.transfers;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.examples.apps.ledger.contracts.Ledger;

/**
 * Where transfers are asked for, and where refusals are noticed.
 *
 * <p>Deliberately thin. Everything a ledger is careful about happens in the writer; this module
 * exists to make the shape obvious — a transfer is a <strong>command</strong>, sent to a queue,
 * which may be refused, and a refusal is a normal outcome rather than an error.
 *
 * <p>The distinction matters more than it looks. Events are named in the past tense and cannot
 * be argued with; commands are requests and can be turned down. Systems that blur the two end up
 * publishing {@code TransferMade} before knowing whether it was, and then need a second event to
 * take it back.
 */
public final class TransferGateway implements AutoCloseable {

    private final AceMq mq;
    private final MessageConsumer rejections;
    private final List<Ledger.TransferRejected> refused = new CopyOnWriteArrayList<>();

    public TransferGateway(AceMq mq) {
        this.mq = mq;
        this.rejections = mq.consume(
                "ledger.rejections", Ledger.TransferRejected.class, message -> refused.add(message.payload()));
    }

    /**
     * Asks for money to move.
     *
     * @param from the account debited
     * @param to the account credited
     * @param amountMinor how much, in whole minor units
     * @param description what a statement will show
     * @return the transfer id, which correlates the command with both entries and any refusal
     */
    public String request(String from, String to, long amountMinor, String description) {
        String transferId = "T-" + UUID.randomUUID().toString().substring(0, 8);
        mq.publisher(Ledger.EXCHANGE, "ledger.transfer.requested", Ledger.Transfer.class)
                .send(new Ledger.Transfer(transferId, from, to, amountMinor, description),
                        Envelope.of("Transfer").correlationId(transferId).build());
        return transferId;
    }

    /** @return the transfers the ledger refused, and why */
    public List<Ledger.TransferRejected> refused() {
        return List.copyOf(refused);
    }

    @Override
    public void close() throws Exception {
        rejections.close();
    }
}
