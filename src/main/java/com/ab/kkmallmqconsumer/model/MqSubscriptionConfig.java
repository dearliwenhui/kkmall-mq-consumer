package com.ab.kkmallmqconsumer.model;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("mq_subscription_config")
public class MqSubscriptionConfig {

    @TableId(type = IdType.AUTO)
    private Long id;
    private String env;
    private String topic;
    private String consumerGroup;
    private String messageType;
    private String sourceSystem;
    private String sourceDb;
    private String sourceTable;
    private String parserType;
    private Integer enabled;
    private String remark;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
