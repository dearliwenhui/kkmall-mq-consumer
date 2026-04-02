package com.ab.kkmallmqconsumer.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class RedisCacheHandlerConfig {

    private String logicalTable;
    private String pkField = "id";
    private String dataKeyPrefix;
    private String indexKeyPrefix;
    private Long ttlSeconds;
    private List<RedisIndexConfig> indexes = new ArrayList<>();
}
