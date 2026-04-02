package com.ab.kkmallmqconsumer.service;

import com.ab.kkmallmqconsumer.model.CdcTopicRule;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ConfigSnapshotService {

    private final TopicConfigService topicConfigService;

    public ConfigSnapshotService(TopicConfigService topicConfigService) {
        this.topicConfigService = topicConfigService;
    }

    public List<CdcTopicRule> snapshot() {
        return topicConfigService.currentRules().values().stream()
                .sorted(java.util.Comparator.comparing(CdcTopicRule::getTopic))
                .toList();
    }
}
