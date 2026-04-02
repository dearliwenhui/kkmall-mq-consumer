package com.ab.kkmallmqconsumer.scheduler;

import com.ab.kkmallmqconsumer.consumer.RocketMqConsumerManager;
import com.ab.kkmallmqconsumer.service.TopicConfigService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConfigRefreshSchedulerTests {

    @Test
    void shouldReconcileConsumersEvenWhenConfigSignatureIsUnchanged() {
        TopicConfigService topicConfigService = mock(TopicConfigService.class);
        RocketMqConsumerManager consumerManager = mock(RocketMqConsumerManager.class);
        when(topicConfigService.refresh()).thenReturn(false);

        ConfigRefreshScheduler scheduler = new ConfigRefreshScheduler(topicConfigService, consumerManager);
        scheduler.refresh();

        verify(topicConfigService).refresh();
        verify(consumerManager).refreshConsumers();
    }
}
