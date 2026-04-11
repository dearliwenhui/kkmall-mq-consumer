package com.ab.kkmallmqconsumer.consumer;

import com.ab.kkmallmqconsumer.config.OrderTimeoutConsumerProperties;
import com.ab.kkmallmqconsumer.model.OrderTimeoutCheckMessage;
import com.ab.kkmallmqconsumer.service.MallApiOrderTimeoutClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

/**
 * RocketMQ 订单超时消息消费者。
 *
 * 当前版本支持运行时热更新：
 * - enabled 开关
 * - nameserver
 * - topic / tag
 * - consumer group
 * - consumeThreadMin / consumeThreadMax
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderTimeoutConsumerManager {

    private final Environment environment;
    private final MallApiOrderTimeoutClient mallApiOrderTimeoutClient;
    private final ObjectMapper objectMapper;

    private volatile DefaultMQPushConsumer consumer;
    private volatile String activeSignature = "";

    /**
     * 根据当前配置刷新消费者实例。
     *
     * 配置未变化：不处理。
     * 配置变更：先关闭旧消费者，再按新配置重建。
     * enabled=false：关闭并保持停用。
     */
    public synchronized void refreshConsumer() {
        OrderTimeoutConsumerProperties properties = currentProperties();
        String nextSignature = buildSignature(properties);

        if (!properties.isEnabled()) {
            if (consumer != null) {
                log.info("Order timeout consumer disabled by config, shutting down current consumer. group={}, topic={}",
                        properties.resolveConsumerGroup(), properties.resolveTopic());
            }
            shutdown();
            activeSignature = nextSignature;
            return;
        }

        if (consumer != null && Objects.equals(activeSignature, nextSignature)) {
            return;
        }

        if (consumer != null) {
            log.info("Order timeout consumer config changed, restarting consumer. oldSignature={}, newSignature={}",
                    activeSignature, nextSignature);
            shutdown();
        }

        startConsumer(properties);
        activeSignature = nextSignature;
    }

    private void startConsumer(OrderTimeoutConsumerProperties properties) {
        try {
            DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(properties.resolveConsumerGroup());
            pushConsumer.setNamesrvAddr(properties.getNameserver());
            pushConsumer.setConsumeThreadMin(properties.getConsumeThreadMin());
            pushConsumer.setConsumeThreadMax(Math.max(properties.getConsumeThreadMax(), properties.getConsumeThreadMin()));
            pushConsumer.subscribe(properties.resolveTopic(), properties.resolveSubscriptionExpression());
            pushConsumer.registerMessageListener(this::consumeMessages);
            pushConsumer.start();
            consumer = pushConsumer;
            log.info("Started order timeout consumer. group={}, topic={}, tag={}, minThreads={}, maxThreads={}",
                    properties.resolveConsumerGroup(),
                    properties.resolveTopic(),
                    properties.resolveSubscriptionExpression(),
                    properties.getConsumeThreadMin(),
                    Math.max(properties.getConsumeThreadMax(), properties.getConsumeThreadMin()));
        } catch (Exception exception) {
            consumer = null;
            throw new IllegalStateException("Failed to start order timeout consumer", exception);
        }
    }

    private ConsumeConcurrentlyStatus consumeMessages(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
        for (MessageExt message : messages) {
            try {
                OrderTimeoutCheckMessage payload = objectMapper.readValue(message.getBody(), OrderTimeoutCheckMessage.class);
                mallApiOrderTimeoutClient.closeExpiredOrder(payload.orderId());
                log.info("Processed order timeout message. orderId={}, orderNo={}", payload.orderId(), payload.orderNo());
            } catch (Exception exception) {
                log.error("Failed to process order timeout message. msgId={}", message.getMsgId(), exception);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    private OrderTimeoutConsumerProperties currentProperties() {
        return Binder.get(environment)
                .bind("kkmall.order-timeout", OrderTimeoutConsumerProperties.class)
                .orElseGet(OrderTimeoutConsumerProperties::new);
    }

    private String buildSignature(OrderTimeoutConsumerProperties properties) {
        return String.join("|",
                String.valueOf(properties.isEnabled()),
                properties.getNameserver(),
                properties.resolveTopic(),
                properties.resolveSubscriptionExpression(),
                properties.resolveConsumerGroup(),
                String.valueOf(properties.getConsumeThreadMin()),
                String.valueOf(Math.max(properties.getConsumeThreadMax(), properties.getConsumeThreadMin())));
    }

    @PreDestroy
    public synchronized void shutdown() {
        if (consumer == null) {
            return;
        }
        try {
            consumer.shutdown();
            log.info("Shutdown order timeout consumer. signature={}", activeSignature);
        } finally {
            consumer = null;
        }
    }
}
