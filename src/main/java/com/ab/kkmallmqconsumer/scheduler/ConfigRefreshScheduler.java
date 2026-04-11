package com.ab.kkmallmqconsumer.scheduler;

import com.ab.kkmallmqconsumer.consumer.OrderTimeoutConsumerManager;
import com.ab.kkmallmqconsumer.consumer.RocketMqConsumerManager;
import com.ab.kkmallmqconsumer.service.TopicConfigService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 周期刷新 consumer 配置。
 *
 * - CDC 订阅：从数据库刷新
 * - 订单超时 consumer：从配置中心/Nacos/环境变量刷新
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConfigRefreshScheduler {

    private final TopicConfigService topicConfigService;
    private final RocketMqConsumerManager consumerManager;
    private final OrderTimeoutConsumerManager orderTimeoutConsumerManager;

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        refresh();
    }

    @Scheduled(fixedDelayString = "${kkmall.mq-consumer.config-refresh-interval-ms:10000}")
    public void refresh() {
        try {
            topicConfigService.refresh();
            consumerManager.refreshConsumers();
        } catch (Exception exception) {
            log.error("Failed to refresh CDC topic configs", exception);
        }

        try {
            orderTimeoutConsumerManager.refreshConsumer();
        } catch (Exception exception) {
            log.error("Failed to refresh order-timeout consumer", exception);
        }
    }
}
