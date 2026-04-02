package com.ab.kkmallmqconsumer.service;

import com.ab.kkmallmqconsumer.config.CdcConsumerProperties;
import com.ab.kkmallmqconsumer.model.CdcTopicRule;
import com.ab.kkmallmqconsumer.model.IndexRule;
import com.ab.kkmallmqconsumer.model.MqHandlerConfig;
import com.ab.kkmallmqconsumer.model.MqSubscriptionConfig;
import com.ab.kkmallmqconsumer.model.RedisCacheHandlerConfig;
import com.ab.kkmallmqconsumer.model.RedisIndexConfig;
import com.ab.kkmallmqconsumer.repository.TopicConfigRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class TopicConfigService {

    private static final String MESSAGE_TYPE_CDC = "CDC";
    private static final String HANDLER_TYPE_REDIS_CACHE = "REDIS_CACHE";

    private final TopicConfigRepository topicConfigRepository;
    private final CdcConsumerProperties properties;
    private final ObjectMapper objectMapper;

    private volatile Map<String, CdcTopicRule> ruleMap = Map.of();
    private volatile String signature = "";

    public synchronized boolean refresh() {
        List<MqSubscriptionConfig> subscriptions = topicConfigRepository.findActiveSubscriptions(properties.getEnv(), MESSAGE_TYPE_CDC);
        List<Long> subscriptionIds = subscriptions.stream().map(MqSubscriptionConfig::getId).toList();
        List<MqHandlerConfig> handlers = topicConfigRepository.findEnabledHandlers(subscriptionIds);

        Map<Long, List<MqHandlerConfig>> handlersBySubscription = handlers.stream()
                .collect(Collectors.groupingBy(MqHandlerConfig::getSubscriptionId, Collectors.toList()));

        Map<String, CdcTopicRule> newRuleMap = new ConcurrentHashMap<>();
        for (MqSubscriptionConfig subscription : subscriptions) {
            CdcTopicRule rule = toRule(subscription, handlersBySubscription.getOrDefault(subscription.getId(), Collections.emptyList()));
            if (rule != null) {
                newRuleMap.put(rule.getTopic(), rule);
            }
        }

        String newSignature = buildSignature(newRuleMap);
        if (newSignature.equals(signature)) {
            return false;
        }

        this.ruleMap = Map.copyOf(newRuleMap);
        this.signature = newSignature;
        log.info("Loaded {} CDC subscription rules for env={}", newRuleMap.size(), properties.getEnv());
        return true;
    }

    public Map<String, CdcTopicRule> currentRules() {
        return ruleMap;
    }

    public CdcTopicRule findByTopic(String topic) {
        return ruleMap.get(topic);
    }

    public String currentSignature() {
        return signature;
    }

    private CdcTopicRule toRule(MqSubscriptionConfig subscription, List<MqHandlerConfig> handlers) {
        MqHandlerConfig redisHandler = handlers.stream()
                .filter(handler -> HANDLER_TYPE_REDIS_CACHE.equalsIgnoreCase(handler.getHandlerType()))
                .findFirst()
                .orElse(null);
        if (redisHandler == null) {
            log.debug("Skip subscription because REDIS_CACHE handler is missing. topic={}", subscription.getTopic());
            return null;
        }

        RedisCacheHandlerConfig cacheConfig = parseRedisHandlerConfig(subscription.getTopic(), redisHandler);
        if (!StringUtils.hasText(cacheConfig.getLogicalTable())) {
            log.warn("Skip subscription because logicalTable is missing. topic={}, handlerId={}", subscription.getTopic(), redisHandler.getId());
            return null;
        }

        return CdcTopicRule.builder()
                .id(subscription.getId())
                .env(subscription.getEnv())
                .topic(subscription.getTopic())
                .dbName(subscription.getSourceDb())
                .tableName(subscription.getSourceTable())
                .logicalTable(cacheConfig.getLogicalTable())
                .pkField(defaultIfBlank(cacheConfig.getPkField(), "id"))
                .consumerGroup(subscription.getConsumerGroup())
                .valueTtlSeconds(cacheConfig.getTtlSeconds())
                .dataKeyPrefix(defaultIfBlank(cacheConfig.getDataKeyPrefix(), properties.getCache().getDataKeyPrefix()))
                .indexKeyPrefix(defaultIfBlank(cacheConfig.getIndexKeyPrefix(), properties.getCache().getIndexKeyPrefix()))
                .indexRules(toIndexRules(cacheConfig.getIndexes()))
                .build();
    }

    private RedisCacheHandlerConfig parseRedisHandlerConfig(String topic, MqHandlerConfig handler) {
        try {
            return objectMapper.readValue(handler.getConfigJson(), RedisCacheHandlerConfig.class);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to parse REDIS_CACHE handler config for topic=" + topic + ", handlerId=" + handler.getId(), exception);
        }
    }

    private List<IndexRule> toIndexRules(List<RedisIndexConfig> indexes) {
        if (indexes == null || indexes.isEmpty()) {
            return List.of();
        }
        List<IndexRule> rules = new ArrayList<>();
        for (RedisIndexConfig index : indexes) {
            if (!StringUtils.hasText(index.getField()) || !StringUtils.hasText(index.getType())) {
                continue;
            }
            rules.add(IndexRule.builder()
                    .fieldName(index.getField())
                    .indexType(index.getType())
                    .build());
        }
        return rules;
    }

    private String buildSignature(Map<String, CdcTopicRule> rules) {
        return rules.values().stream()
                .sorted(java.util.Comparator.comparing(CdcTopicRule::getTopic))
                .map(rule -> rule.getTopic() + "|" + rule.getConsumerGroup() + "|" + rule.getLogicalTable() + "|"
                        + rule.getIndexRules().stream()
                        .map(index -> index.getFieldName() + ":" + index.getIndexType())
                        .sorted()
                        .collect(Collectors.joining(",")))
                .collect(Collectors.joining(";"));
    }

    private String defaultIfBlank(String value, String fallback) {
        return StringUtils.hasText(value) ? value : fallback;
    }
}
