package com.ab.kkmallmqconsumer.service;

import com.ab.kkmallmqconsumer.model.CdcEvent;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@RequiredArgsConstructor
public class CdcMessageParser {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final ObjectMapper objectMapper;

    public CdcEvent parse(String body) {
        try {
            JsonNode root = objectMapper.readTree(body);
            JsonNode eventNode = payloadNode(root);
            return CdcEvent.builder()
                    .op(text(eventNode, "op"))
                    .before(map(eventNode.get("before")))
                    .after(map(eventNode.get("after")))
                    .db(text(eventNode.path("source"), "db"))
                    .table(text(eventNode.path("source"), "table"))
                    .tsMs(longValue(eventNode.get("ts_ms")))
                    .build();
        } catch (Exception exception) {
            throw new IllegalArgumentException("Failed to parse CDC message", exception);
        }
    }

    private JsonNode payloadNode(JsonNode root) {
        JsonNode payload = root == null ? null : root.get("payload");
        if (payload != null && !payload.isNull() && payload.isObject()) {
            return payload;
        }
        return root;
    }

    private Map<String, Object> map(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        return objectMapper.convertValue(node, MAP_TYPE);
    }

    private String text(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private Long longValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        return node.asLong();
    }
}
