package io.temporal.demo.wealthmanagement.claimcheck;

import io.temporal.api.common.v1.Payload;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

/** Stores Payloads in Redis as their serialized protobuf bytes, as the Python codec does. */
public class ProtobufRedisSerializer implements RedisSerializer<Payload> {
    @Override
    public byte[] serialize(Payload payload) throws SerializationException {
        return payload == null ? null : payload.toByteArray();
    }

    @Override
    public Payload deserialize(byte[] bytes) throws SerializationException {
        if (bytes == null) {
            return null;
        }
        try {
            return Payload.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new SerializationException("Could not deserialize the Payload", e);
        }
    }
}
