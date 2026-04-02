package com.ab.kkmallmqconsumer.consumer;

import com.ab.kkmallmqconsumer.config.CdcConsumerProperties;
import com.ab.kkmallmqconsumer.dispatcher.CdcMessageDispatcher;
import com.ab.kkmallmqconsumer.service.TopicConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class RocketMqConsumerManagerTests {

    @AfterEach
    void tearDown() {
        System.clearProperty("rocketmq.namesrv.addr");
        System.clearProperty("NAMESRV_ADDR");
    }

    @Test
    void shouldFallbackToSystemPropertyWhenConfiguredNameserverIsInvalidLiteral() {
        CdcConsumerProperties properties = new CdcConsumerProperties();
        properties.getRocketmq().setNameserver("null");
        System.setProperty("rocketmq.namesrv.addr", "10.0.0.166:9876");

        RocketMqConsumerManager manager = new RocketMqConsumerManager(
                mock(TopicConfigService.class),
                mock(CdcMessageDispatcher.class),
                properties);

        assertEquals("10.0.0.166:9876", manager.resolveNameserver());
    }

    @Test
    void shouldRejectUnresolvedPlaceholderWhenNoFallbackExists() {
        CdcConsumerProperties properties = new CdcConsumerProperties();
        properties.getRocketmq().setNameserver("${KKMALL_MQ_CONSUMER_NAMESRV_ADDR}");

        RocketMqConsumerManager manager = new RocketMqConsumerManager(
                mock(TopicConfigService.class),
                mock(CdcMessageDispatcher.class),
                properties);

        assertThrows(IllegalStateException.class, manager::resolveNameserver);
    }
}
