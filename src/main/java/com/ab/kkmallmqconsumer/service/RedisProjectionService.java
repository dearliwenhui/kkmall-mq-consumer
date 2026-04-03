package com.ab.kkmallmqconsumer.service;

import com.ab.kkmallmqconsumer.common.CacheKeys;
import com.ab.kkmallmqconsumer.config.CdcConsumerProperties;
import com.ab.kkmallmqconsumer.model.CdcEvent;
import com.ab.kkmallmqconsumer.model.CdcTopicRule;
import com.ab.kkmallmqconsumer.model.IndexRule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisProjectionService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final CdcConsumerProperties properties;

    public void apply(CdcTopicRule rule, CdcEvent event) {
        if (rule == null || event == null) {
            return;
        }

        if ("d".equalsIgnoreCase(event.getOp())) {
            delete(rule, event.getBefore());
            return;
        }

        Map<String, Object> after = event.getAfter();
        if (after == null || after.isEmpty()) {
            log.warn("Skip CDC event because after payload is empty. topic={}, op={}", rule.getTopic(), event.getOp());
            return;
        }

        if ("u".equalsIgnoreCase(event.getOp()) && event.getBefore() != null) {
            removeIndexes(rule, event.getBefore());
        }

        upsert(rule, event, after);
    }

    private void upsert(CdcTopicRule rule, CdcEvent event, Map<String, Object> data) {
        Object id = data.get(rule.getPkField());
        if (id == null) {
            log.warn("Skip CDC upsert because pk field is missing. topic={}, pkField={}", rule.getTopic(), rule.getPkField());
            return;
        }

        String dataKey = CacheKeys.dataKey(rule.getDataKeyPrefix(), properties.getEnv(), rule.getLogicalTable(), id);
        Long incomingVersion = extractVersion(data, event.getTsMs());
        if (!shouldWrite(dataKey, incomingVersion)) {
            return;
        }

        Map<String, Object> payload = new LinkedHashMap<>(data);
        payload.put("version", incomingVersion);

        redisTemplate.opsForValue().set(dataKey, payload);
        if (rule.getValueTtlSeconds() != null && rule.getValueTtlSeconds() > 0) {
            redisTemplate.expire(dataKey, rule.getValueTtlSeconds(), TimeUnit.SECONDS);
        }
        writeIndexes(rule, data, id, rule.getValueTtlSeconds());
    }

    private boolean shouldWrite(String key, Long incomingVersion) {
        Object existing = redisTemplate.opsForValue().get(key);
        if (!(existing instanceof Map<?, ?> existingMap)) {
            return true;
        }

        Long currentVersion = toLong(existingMap.get("version"));
        if (currentVersion == null) {
            return true;
        }
        if (incomingVersion == null) {
            return false;
        }
        return incomingVersion > currentVersion;
    }

    private void delete(CdcTopicRule rule, Map<String, Object> before) {
        if (before == null || before.isEmpty()) {
            return;
        }
        removeIndexes(rule, before);
        Object id = before.get(rule.getPkField());
        if (id == null) {
            return;
        }
        String dataKey = CacheKeys.dataKey(rule.getDataKeyPrefix(), properties.getEnv(), rule.getLogicalTable(), id);
        redisTemplate.delete(dataKey);
    }

    private void removeIndexes(CdcTopicRule rule, Map<String, Object> data) {
        Object id = data.get(rule.getPkField());
        if (id == null) {
            return;
        }
        for (IndexRule indexRule : safeIndexes(rule)) {
            Object value = data.get(indexRule.getFieldName());
            if (value == null) {
                continue;
            }
            String indexKey = CacheKeys.indexKey(rule.getIndexKeyPrefix(), properties.getEnv(), rule.getLogicalTable(), indexRule.getFieldName(), value);
            if (indexRule.unique()) {
                redisTemplate.delete(indexKey);
            } else {
                redisTemplate.opsForSet().remove(indexKey, String.valueOf(id));
            }
        }
    }

    private void writeIndexes(CdcTopicRule rule, Map<String, Object> data, Object id, Long ttlSeconds) {
        for (IndexRule indexRule : safeIndexes(rule)) {
            Object value = data.get(indexRule.getFieldName());
            if (value == null) {
                continue;
            }
            String indexKey = CacheKeys.indexKey(rule.getIndexKeyPrefix(), properties.getEnv(), rule.getLogicalTable(), indexRule.getFieldName(), value);
            if (indexRule.unique()) {
                redisTemplate.opsForValue().set(indexKey, String.valueOf(id));
            } else {
                redisTemplate.opsForSet().add(indexKey, String.valueOf(id));
            }
            if (ttlSeconds != null && ttlSeconds > 0) {
                redisTemplate.expire(indexKey, ttlSeconds, TimeUnit.SECONDS);
            }
        }
    }

    private List<IndexRule> safeIndexes(CdcTopicRule rule) {
        return rule.getIndexRules() == null ? List.of() : rule.getIndexRules();
    }

    private Long extractVersion(Map<String, Object> data, Long eventTsMs) {
        Long entityVersion = toLong(data.get("version"));
        if (entityVersion != null) {
            return entityVersion;
        }

        Object updateTime = data.get("update_time");
        if (updateTime == null) {
            updateTime = data.get("updateTime");
        }
        Long parsed = parseTime(updateTime);
        return parsed != null ? parsed : eventTsMs;
    }

    private Long parseTime(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        String text = String.valueOf(value);
        if (!StringUtils.hasText(text)) {
            return null;
        }
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException ignore) {
        }
        try {
            return LocalDateTime.parse(text).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (DateTimeParseException ignore) {
            return null;
        }
    }

    private Long toLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException exception) {
            return null;
        }
    }

}
