package io.temporal.demo.wealthmanagement.claimcheck;

import com.google.protobuf.ByteString;
import io.temporal.api.common.v1.Payload;
import io.temporal.common.converter.DataConverterException;
import io.temporal.common.converter.EncodingKeys;
import io.temporal.payload.codec.PayloadCodec;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;

/**
 * Claim check payload codec: stores each payload in Redis under a random ID and sends only the ID to
 * Temporal. The metadata and the stored bytes (the serialized Payload protobuf) are the same as the
 * Python codec's (src/temporal_supervisor/claim_check/claim_check_codec.py), so either side can
 * decode the other's payloads, and the Python codec server can show them in the Temporal UI.
 * Payloads that weren't claim-checked pass through unchanged.
 */
@Component
public class ClaimCheckCodec implements PayloadCodec {
    static final ByteString ENCODING = ByteString.copyFromUtf8("claim-checked");
    static final String CODEC_KEY = "temporal.io/claim-check-codec";
    static final ByteString CODEC_VERSION = ByteString.copyFromUtf8("v1");

    private final RedisTemplate<String, Payload> redisTemplate;

    public ClaimCheckCodec(RedisTemplate<String, Payload> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Nonnull
    @Override
    public List<Payload> encode(@Nonnull List<Payload> payloads) {
        return payloads.stream().map(this::encodePayload).toList();
    }

    @Nonnull
    @Override
    public List<Payload> decode(@Nonnull List<Payload> payloads) {
        return payloads.stream().map(this::decodePayload).toList();
    }

    private Payload encodePayload(Payload payload) {
        String id = UUID.randomUUID().toString();
        redisTemplate.opsForValue().set(id, payload);
        return Payload.newBuilder()
                .putMetadata(EncodingKeys.METADATA_ENCODING_KEY, ENCODING)
                .putMetadata(CODEC_KEY, CODEC_VERSION)
                .setData(ByteString.copyFromUtf8(id))
                .build();
    }

    private Payload decodePayload(Payload payload) {
        if (!CODEC_VERSION.equals(payload.getMetadataOrDefault(CODEC_KEY, ByteString.EMPTY))) {
            return payload;
        }
        String id = payload.getData().toStringUtf8();
        Payload stored;
        try {
            stored = redisTemplate.opsForValue().get(id);
        } catch (RuntimeException e) {
            throw new DataConverterException("Could not read claim-checked payload " + id + " from Redis", e);
        }
        if (stored == null) {
            throw new DataConverterException("Claim-checked payload " + id + " not found in Redis", (Throwable) null);
        }
        return stored;
    }
}
