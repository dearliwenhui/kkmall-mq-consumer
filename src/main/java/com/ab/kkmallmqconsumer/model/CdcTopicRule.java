package com.ab.kkmallmqconsumer.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class CdcTopicRule {
    private Long id;
    private String env;
    private String topic;
    private String dbName;
    private String tableName;
    private String logicalTable;
    private String pkField;
    private String consumerGroup;
    private Long valueTtlSeconds;
    private String dataKeyPrefix;
    private String indexKeyPrefix;
    private List<IndexRule> indexRules;
}
