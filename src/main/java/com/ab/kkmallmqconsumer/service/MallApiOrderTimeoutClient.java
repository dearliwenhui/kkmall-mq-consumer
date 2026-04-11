package com.ab.kkmallmqconsumer.service;

import com.ab.kkmallmqconsumer.config.OrderTimeoutConsumerProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

/**
 * 调用 mall-api 内部关单接口的轻量客户端。
 *
 * 每次请求前都从当前环境重新绑定配置，确保 Nacos 动态刷新后立刻生效。
 */
@Service
@RequiredArgsConstructor
public class MallApiOrderTimeoutClient {

    private static final String INTERNAL_TOKEN_HEADER = "X-Internal-Token";

    private final Environment environment;
    private final RestClient restClient = RestClient.create();

    /**
     * 将超时关单动作回调给 mall-api 执行。
     */
    public void closeExpiredOrder(Long orderId) {
        OrderTimeoutConsumerProperties properties = currentProperties();
        restClient.post()
                .uri(normalizeBaseUrl(properties.getMallApiBaseUrl()) + "/api/internal/orders/{id}/timeout-close", orderId)
                .header(INTERNAL_TOKEN_HEADER, properties.getInternalApiToken())
                .retrieve()
                .toBodilessEntity();
    }

    private OrderTimeoutConsumerProperties currentProperties() {
        return Binder.get(environment)
                .bind("kkmall.order-timeout", OrderTimeoutConsumerProperties.class)
                .orElseGet(OrderTimeoutConsumerProperties::new);
    }

    private String normalizeBaseUrl(String baseUrl) {
        return baseUrl != null && baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
    }
}
