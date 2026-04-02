package com.ab.kkmallmqconsumer.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class IndexRule {
    private String fieldName;
    private String indexType;

    public boolean unique() {
        return "UNIQUE".equalsIgnoreCase(indexType);
    }
}
