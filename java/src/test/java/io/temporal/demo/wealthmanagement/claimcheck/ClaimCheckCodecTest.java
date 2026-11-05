package io.temporal.demo.wealthmanagement.claimcheck;

import com.google.protobuf.ByteString;
import io.temporal.api.common.v1.Payload;
import io.temporal.common.converter.DataConverterException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** The codec against an in-memory stand-in for Redis. */
class ClaimCheckCodecTest {
    private final Map<String, byte[]> redis = new HashMap<>();
    private ClaimCheckCodec codec;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        RedisTemplate<String, Payload> template = mock(RedisTemplate.class);
        ValueOperations<String, Payload> values = mock(ValueOperations.class);
        when(template.opsForValue()).thenReturn(values);
        ProtobufRedisSerializer serializer = new ProtobufRedisSerializer();
        doAnswer(call -> redis.put(call.getArgument(0), serializer.serialize(call.getArgument(1))))
                .when(values).set(anyString(), any(Payload.class));
        when(values.get(anyString())).thenAnswer(call -> serializer.deserialize(redis.get((String) call.getArgument(0))));
        codec = new ClaimCheckCodec(template);
    }

    private static Payload json(String data) {
        return Payload.newBuilder()
                .putMetadata("encoding", ByteString.copyFromUtf8("json/plain"))
                .setData(ByteString.copyFromUtf8(data))
                .build();
    }

    @Test
    void storesThePayloadInRedisAndSendsOnlyItsId() throws Exception {
        Payload original = json("{\"client_id\":\"123\"}");
        Payload encoded = codec.encode(List.of(original)).get(0);

        // Same metadata as the Python codec (claim_check_codec.py)
        assertEquals("claim-checked", encoded.getMetadataOrThrow("encoding").toStringUtf8());
        assertEquals("v1", encoded.getMetadataOrThrow("temporal.io/claim-check-codec").toStringUtf8());
        String id = encoded.getData().toStringUtf8();
        // The stored value is the serialized Payload protobuf, as the Python codec stores it
        assertEquals(original, Payload.parseFrom(redis.get(id)));

        assertEquals(List.of(original), codec.decode(List.of(encoded)));
    }

    @Test
    void passesThroughPayloadsThatWereNotClaimChecked() {
        Payload plain = json("\"hello\"");
        assertEquals(List.of(plain), codec.decode(List.of(plain)));
    }

    @Test
    void failsClearlyWhenThePayloadIsMissingFromRedis() {
        Payload encoded = codec.encode(List.of(json("1"))).get(0);
        redis.clear();
        assertThrows(DataConverterException.class, () -> codec.decode(List.of(encoded)));
    }
}
