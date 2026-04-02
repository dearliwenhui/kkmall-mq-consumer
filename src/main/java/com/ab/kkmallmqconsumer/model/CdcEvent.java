package com.ab.kkmallmqconsumer.model;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class CdcEvent {
    private String op;
    private Map<String, Object> before;
    private Map<String, Object> after;
    private String db;
    private String table;
    private Long tsMs;
}
