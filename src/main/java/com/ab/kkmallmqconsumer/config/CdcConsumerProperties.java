package com.ab.kkmallmqconsumer.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties(prefix = "kkmall.mq-consumer")
public class CdcConsumerProperties {

    @NotBlank
    private String env = "dev";

    @Min(1000)
    private long configRefreshIntervalMs = 10000L;

    private final RocketMq rocketmq = new RocketMq();

    private final Cache cache = new Cache();

    @Data
    public static class RocketMq {
        @NotBlank
        private String nameserver = "rocketmq-nameserver.rocketmq.svc.cluster.local:9876";
        @Min(1)
        private int consumeThreadMin = 2;
        @Min(1)
        private int consumeThreadMax = 8;
    }

    @Data
    public static class Cache {
        @NotBlank
        private String dataKeyPrefix = "cdc";
        @NotBlank
        private String indexKeyPrefix = "cdc_idx";
        private boolean compareBeforeWrite = true;
    }
}
