package com.ab.kkmallmqconsumer;

import com.ab.kkmallmqconsumer.config.CdcConsumerProperties;
import com.ab.kkmallmqconsumer.config.OrderTimeoutConsumerProperties;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@MapperScan("com.ab.kkmallmqconsumer.mapper")
@EnableScheduling
@EnableConfigurationProperties({CdcConsumerProperties.class, OrderTimeoutConsumerProperties.class})
public class KkmallMqConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KkmallMqConsumerApplication.class, args);
    }
}
