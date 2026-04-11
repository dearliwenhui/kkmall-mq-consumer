package com.ab.kkmallmqconsumer.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;

/**
 * 订单超时消费者配置。
 *
 * 这类消费者不走数据库动态订阅，而是由配置中心/Nacos/环境变量统一控制。
 */
@Data
@Validated
@ConfigurationProperties(prefix = "kkmall.order-timeout")
public class OrderTimeoutConsumerProperties {

    @NotBlank
    private String env = "dev";

    private boolean enabled = true;

    @NotBlank
    private String nameserver = "rocketmq-nameserver.rocketmq.svc.cluster.local:9876";

    @NotBlank
    private String topic = "kkmall_order_timeout";

    private String tag = "check";

    @NotBlank
    private String consumerGroup = "kkmall-mq-consumer-order-timeout";

    @Min(1)
    private int consumeThreadMin = 1;

    @Min(1)
    private int consumeThreadMax = 4;

    @NotBlank
    private String mallApiBaseUrl = "http://kkmall-mall-api:38081";

    @NotBlank
    private String internalApiToken = "kkmall-order-timeout-dev-token";

    /**
     * 解析带环境前缀的 Topic 名称，例如 `prod_kkmall_order_timeout`。
     */
    public String resolveTopic() {
        return applyEnvPrefix(topic);
    }

    /**
     * 解析带环境后缀的 Consumer Group，例如 `kkmall-mq-consumer-order-timeout-prod`。
     */
    public String resolveConsumerGroup() {
        return applyEnvSuffix(consumerGroup);
    }

    /**
     * 订阅表达式，未配置 tag 时退化为全部匹配。
     */
    public String resolveSubscriptionExpression() {
        return StringUtils.hasText(tag) ? tag : "*";
    }

    private String applyEnvPrefix(String value) {
        if (!StringUtils.hasText(value) || !StringUtils.hasText(env)) {
            return value;
        }
        String normalizedEnv = env.trim().toLowerCase();
        String prefix = normalizedEnv + "_";
        return value.startsWith(prefix) ? value : prefix + value;
    }

    private String applyEnvSuffix(String value) {
        if (!StringUtils.hasText(value) || !StringUtils.hasText(env)) {
            return value;
        }
        String normalizedEnv = env.trim().toLowerCase();
        String suffix = "-" + normalizedEnv;
        return value.endsWith(suffix) ? value : value + suffix;
    }
}
