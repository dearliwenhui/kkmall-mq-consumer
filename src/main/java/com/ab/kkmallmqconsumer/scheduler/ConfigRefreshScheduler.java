package com.ab.kkmallmqconsumer.scheduler;

import com.ab.kkmallmqconsumer.consumer.RocketMqConsumerManager;
import com.ab.kkmallmqconsumer.service.TopicConfigService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConfigRefreshScheduler {

    private final TopicConfigService topicConfigService;
    private final RocketMqConsumerManager consumerManager;

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
    }
}
