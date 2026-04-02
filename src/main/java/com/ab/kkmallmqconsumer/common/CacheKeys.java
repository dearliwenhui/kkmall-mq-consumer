package com.ab.kkmallmqconsumer.common;

public final class CacheKeys {

    private CacheKeys() {
    }

    public static String dataKey(String prefix, String env, String logicalTable, Object id) {
        return prefix + ":" + env + ":" + logicalTable + ":" + id;
    }

    public static String indexKey(String prefix, String env, String logicalTable, String field, Object value) {
        return prefix + ":" + env + ":" + logicalTable + ":" + field + ":" + value;
    }
}
