package io.temporal.demo.wealthmanagement.claimcheck;

import io.temporal.api.common.v1.Payload;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/** The Redis used by the claim check codec (spring.data.redis in application.yaml). */
@Configuration
public class RedisConfig {
    @Bean
    public RedisTemplate<String, Payload> claimCheckRedisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Payload> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new ProtobufRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }
}
