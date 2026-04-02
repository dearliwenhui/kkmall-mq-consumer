package com.ab.kkmallmqconsumer.consumer;

import com.ab.kkmallmqconsumer.config.CdcConsumerProperties;
import com.ab.kkmallmqconsumer.dispatcher.CdcMessageDispatcher;
import com.ab.kkmallmqconsumer.model.CdcTopicRule;
import com.ab.kkmallmqconsumer.service.TopicConfigService;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class RocketMqConsumerManager {

    private final TopicConfigService topicConfigService;
    private final CdcMessageDispatcher dispatcher;
    private final CdcConsumerProperties properties;

    private final Map<String, DefaultMQPushConsumer> consumers = new ConcurrentHashMap<>();
    private volatile String activeSignature = "";

    public synchronized void refreshConsumers() {
        Map<String, CdcTopicRule> rules = topicConfigService.currentRules();
        String currentSignature = topicConfigService.currentSignature();
        if (rules.isEmpty()) {
            shutdownAll();
            activeSignature = currentSignature;
            return;
        }
        if (currentSignature.equals(activeSignature)) {
            return;
        }

        shutdownAll();
        Map<String, List<CdcTopicRule>> groupedRules = new ArrayList<>(rules.values()).stream()
                .collect(Collectors.groupingBy(CdcTopicRule::getConsumerGroup));

        groupedRules.forEach(this::startConsumer);
        activeSignature = currentSignature;
    }

    private void startConsumer(String group, List<CdcTopicRule> rules) {
        try {
            String nameserver = resolveNameserver();

            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
            consumer.setNamesrvAddr(nameserver);
            consumer.setConsumeThreadMin(properties.getRocketmq().getConsumeThreadMin());
            consumer.setConsumeThreadMax(properties.getRocketmq().getConsumeThreadMax());

            for (CdcTopicRule rule : rules) {
                consumer.subscribe(rule.getTopic(), "*");
            }

            consumer.registerMessageListener(this::consumeMessages);
            consumer.start();
            consumers.put(group, consumer);
            log.info("Started RocketMQ consumer. group={}, nameserver={}, topics={}", group, nameserver,
                    rules.stream().map(CdcTopicRule::getTopic).sorted().toList());
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to start RocketMQ consumer for group=" + group, exception);
        }
    }

    String resolveNameserver() {
        return Arrays.stream(new String[]{
                        properties.getRocketmq().getNameserver(),
                        System.getenv("KKMALL_MQ_CONSUMER_NAMESRV_ADDR"),
                        System.getenv("NAMESRV_ADDR"),
                        System.getProperty("rocketmq.namesrv.addr"),
                        System.getProperty("NAMESRV_ADDR")
                })
                .map(this::normalizeNameserver)
                .filter(StringUtils::hasText)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "RocketMQ nameserver is blank or invalid. Configure kkmall.mq-consumer.rocketmq.nameserver or KKMALL_MQ_CONSUMER_NAMESRV_ADDR/NAMESRV_ADDR"));
    }

    String normalizeNameserver(String nameserver) {
        if (!StringUtils.hasText(nameserver)) {
            return null;
        }

        String normalized = nameserver.trim();
        if (!StringUtils.hasText(normalized)) {
            return null;
        }
        if ("null".equalsIgnoreCase(normalized) || "undefined".equalsIgnoreCase(normalized)) {
            return null;
        }
        if (normalized.startsWith("${") && normalized.endsWith("}")) {
            return null;
        }
        return normalized;
    }

    private ConsumeConcurrentlyStatus consumeMessages(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
        try {
            for (MessageExt message : messages) {
                dispatcher.dispatch(message.getTopic(), new String(message.getBody(), StandardCharsets.UTF_8));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (Exception exception) {
            log.error("Failed to consume RocketMQ messages", exception);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    @PreDestroy
    public synchronized void shutdownAll() {
        consumers.forEach((group, consumer) -> {
            try {
                consumer.shutdown();
                log.info("Shutdown RocketMQ consumer. group={}", group);
            } catch (Exception exception) {
                log.warn("Failed to shutdown RocketMQ consumer. group={}", group, exception);
            }
        });
        consumers.clear();
    }
}
