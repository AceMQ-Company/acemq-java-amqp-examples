package org.acemq.examples.advanced;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Locale;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;

/**
 * A codec that encrypts whatever another codec produced.
 *
 * <p>Transport security protects a message while it is moving. It does nothing about a
 * message sitting in a queue that an operator, a backup, or anyone with the management UI
 * can read. Where the payload must be opaque to the broker itself, it has to arrive
 * already encrypted — and a {@link Codec} is the seam for that, because it is the last
 * thing to touch the bytes on the way out and the first on the way in.
 *
 * <p>This wraps a delegate rather than serialising anything itself, so the choice of
 * format stays independent of the choice to encrypt. JSON in, AES-GCM out.
 *
 * <p><strong>This is an example, not a security product.</strong> Real use needs the key
 * to come from a key management service, a key identifier on each message so keys can be
 * rotated without a flag day, and a decision about what the operators who can no longer
 * read the queue are supposed to do when they need to debug it. Those are the hard parts
 * and none of them are here.
 */
final class EncryptingCodec implements Codec {

    /**
     * Deliberately not {@code ...+json}, even though the plaintext underneath is JSON.
     *
     * <p>A {@code +json} suffix is a promise that the bytes on the wire are JSON, and every
     * JSON-aware consumer reads it that way -- this library's own codec volunteers for any
     * {@code application/*+json}. These bytes are ciphertext. Naming them {@code +json}
     * makes the JSON codec offer to decode them, which is how a message ends up failing in
     * a parser rather than being refused by a codec that knows it cannot help.
     *
     * <p>The content type describes the wire format, not what is under the encryption.
     */
    static final String CONTENT_TYPE = "application/vnd.acemq.encrypted";

    private static final int NONCE_BYTES = 12;
    private static final int TAG_BITS = 128;

    private final Codec delegate;
    private final SecretKey key;
    private final SecureRandom random = new SecureRandom();

    EncryptingCodec(Codec delegate, SecretKey key) {
        this.delegate = delegate;
        this.key = key;
    }

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public byte[] encode(Object payload) {
        byte[] plaintext = delegate.encode(payload);
        // A fresh nonce per message, prepended to the ciphertext. Reusing one with GCM
        // does not merely weaken the encryption, it forfeits it -- two messages under the
        // same key and nonce leak their difference outright.
        byte[] nonce = new byte[NONCE_BYTES];
        random.nextBytes(nonce);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, nonce));
            byte[] ciphertext = cipher.doFinal(plaintext);
            byte[] framed = new byte[nonce.length + ciphertext.length];
            System.arraycopy(nonce, 0, framed, 0, nonce.length);
            System.arraycopy(ciphertext, 0, framed, nonce.length, ciphertext.length);
            return framed;
        } catch (Exception e) {
            // Deliberately without the payload in the message. An exception that helpfully
            // prints what could not be encrypted writes the plaintext to the log, which is
            // the one place it was never supposed to reach.
            throw new AceMqException("could not encrypt a " + payload.getClass().getName(), e);
        }
    }

    @Override
    public <T> T decode(byte[] body, Class<T> target) {
        if (body.length <= NONCE_BYTES) {
            throw new AceMqException("this message is too short to carry a nonce and a tag,"
                    + " so it was not written by this codec");
        }
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key,
                    new GCMParameterSpec(TAG_BITS, Arrays.copyOf(body, NONCE_BYTES)));
            byte[] plaintext = cipher.doFinal(body, NONCE_BYTES, body.length - NONCE_BYTES);
            return delegate.decode(plaintext, target);
        } catch (AceMqException e) {
            throw e;
        } catch (Exception e) {
            // GCM authenticates as well as encrypts, so this is also what a tampered
            // message looks like. Both are the same answer: do not hand it to the
            // application.
            throw new AceMqException("could not decrypt a message onto " + target.getName()
                    + ". Either it was encrypted with a different key, or it was altered"
                    + " after it was written.", e);
        }
    }

    @Override
    public boolean canDecode(String contentType) {
        // Only its own. Volunteering for anything else would mean trying to decrypt
        // plaintext and reporting the failure as a decode error, which sends whoever is
        // debugging it in precisely the wrong direction.
        return contentType != null
                && contentType.toLowerCase(Locale.ROOT).startsWith(CONTENT_TYPE);
    }

    /** Only for showing what the broker would hold. */
    static String preview(byte[] body) {
        String text = new String(body, StandardCharsets.UTF_8);
        return text.codePoints().anyMatch(c -> c < 0x20 || c > 0x7e)
                ? "<" + body.length + " bytes of ciphertext>"
                : text;
    }
}
