package com.ab.kkmallmqconsumer.dispatcher;

import com.ab.kkmallmqconsumer.model.CdcTopicRule;
import com.ab.kkmallmqconsumer.service.CdcMessageParser;
import com.ab.kkmallmqconsumer.service.RedisProjectionService;
import com.ab.kkmallmqconsumer.service.TopicConfigService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CdcMessageDispatcher {

    private final TopicConfigService topicConfigService;
    private final CdcMessageParser cdcMessageParser;
    private final RedisProjectionService redisProjectionService;

    public void dispatch(String topic, String body) {
        CdcTopicRule rule = topicConfigService.findByTopic(topic);
        if (rule == null) {
            log.warn("Skip message because topic rule is missing. topic={}", topic);
            return;
        }
        redisProjectionService.apply(rule, cdcMessageParser.parse(body));
    }
}
