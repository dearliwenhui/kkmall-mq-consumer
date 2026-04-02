package com.ab.kkmallmqconsumer.model;

import lombok.Data;

@Data
public class RedisIndexConfig {
    private String field;
    private String type;
}
