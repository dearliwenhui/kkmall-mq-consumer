package com.ab.kkmallmqconsumer.model;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("mq_handler_config")
public class MqHandlerConfig {

    @TableId(type = IdType.AUTO)
    private Long id;
    private Long subscriptionId;
    private String handlerType;
    private String handlerName;
    private String configJson;
    private Integer enabled;
    private Integer sortOrder;
    private String remark;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
