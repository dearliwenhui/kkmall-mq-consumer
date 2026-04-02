package com.ab.kkmallmqconsumer.repository;

import com.ab.kkmallmqconsumer.mapper.MqHandlerConfigMapper;
import com.ab.kkmallmqconsumer.mapper.MqSubscriptionConfigMapper;
import com.ab.kkmallmqconsumer.model.MqHandlerConfig;
import com.ab.kkmallmqconsumer.model.MqSubscriptionConfig;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class TopicConfigRepository {

    private final MqSubscriptionConfigMapper mqSubscriptionConfigMapper;
    private final MqHandlerConfigMapper mqHandlerConfigMapper;

    public List<MqSubscriptionConfig> findActiveSubscriptions(String env, String messageType) {
        LambdaQueryWrapper<MqSubscriptionConfig> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(MqSubscriptionConfig::getEnv, env)
                .eq(MqSubscriptionConfig::getEnabled, 1)
                .eq(MqSubscriptionConfig::getMessageType, messageType)
                .orderByAsc(MqSubscriptionConfig::getId);
        return mqSubscriptionConfigMapper.selectList(wrapper);
    }

    public List<MqHandlerConfig> findEnabledHandlers(List<Long> subscriptionIds) {
        if (subscriptionIds == null || subscriptionIds.isEmpty()) {
            return Collections.emptyList();
        }
        LambdaQueryWrapper<MqHandlerConfig> wrapper = new LambdaQueryWrapper<>();
        wrapper.in(MqHandlerConfig::getSubscriptionId, subscriptionIds)
                .eq(MqHandlerConfig::getEnabled, 1)
                .orderByAsc(MqHandlerConfig::getSortOrder)
                .orderByAsc(MqHandlerConfig::getId);
        return mqHandlerConfigMapper.selectList(wrapper);
    }
}
