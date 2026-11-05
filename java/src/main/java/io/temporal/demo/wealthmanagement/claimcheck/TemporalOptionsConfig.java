package io.temporal.demo.wealthmanagement.claimcheck;

import io.temporal.client.WorkflowClientOptions;
import io.temporal.common.converter.CodecDataConverter;
import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.demo.wealthmanagement.common.ServerInfo;
import io.temporal.spring.boot.TemporalOptionsCustomizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Nonnull;
import java.util.List;

/** Adds the claim check codec to the worker's WorkflowClient when USE_CLAIM_CHECK=true. */
@Configuration
public class TemporalOptionsConfig {
    private static final Logger logger = LoggerFactory.getLogger(TemporalOptionsConfig.class);

    @Bean
    public TemporalOptionsCustomizer<WorkflowClientOptions.Builder> claimCheckClientOptions(ClaimCheckCodec claimCheckCodec) {
        return new TemporalOptionsCustomizer<>() {
            @Nonnull
            @Override
            public WorkflowClientOptions.Builder customize(@Nonnull WorkflowClientOptions.Builder optionsBuilder) {
                if (ServerInfo.useClaimCheck()) {
                    logger.info("Using the claim check codec (payloads stored in Redis)");
                    optionsBuilder.setDataConverter(new CodecDataConverter(
                            DefaultDataConverter.newDefaultInstance(), List.of(claimCheckCodec)));
                }
                return optionsBuilder;
            }
        };
    }
}
