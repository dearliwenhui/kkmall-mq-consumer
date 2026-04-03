package com.ab.kkmallmqconsumer.service;

import com.ab.kkmallmqconsumer.model.CdcEvent;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;
import java.util.LinkedHashMap;
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
            JsonNode schemaNode = schemaNode(root);

            return CdcEvent.builder()
                    .op(text(eventNode, "op"))
                    .before(map(eventNode.get("before"), fieldSchema(schemaNode, "before")))
                    .after(map(eventNode.get("after"), fieldSchema(schemaNode, "after")))
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

    private JsonNode schemaNode(JsonNode root) {
        JsonNode schema = root == null ? null : root.get("schema");
        if (schema != null && !schema.isNull() && schema.isObject()) {
            return schema;
        }
        return null;
    }

    private Map<String, Object> map(JsonNode node, JsonNode structSchema) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isObject()) {
            return objectMapper.convertValue(node, MAP_TYPE);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        node.fields().forEachRemaining(entry -> {
            JsonNode childSchema = fieldSchema(structSchema, entry.getKey());
            result.put(entry.getKey(), convertValue(entry.getValue(), childSchema));
        });
        return result;
    }

    private Object convertValue(JsonNode node, JsonNode fieldSchema) {
        if (node == null || node.isNull()) {
            return null;
        }

        if (isDecimalSchema(fieldSchema) && node.isTextual()) {
            return decodeDecimal(node.asText(), decimalScale(fieldSchema));
        }

        return objectMapper.convertValue(node, Object.class);
    }

    private boolean isDecimalSchema(JsonNode schema) {
        if (schema == null || schema.isNull()) {
            return false;
        }
        return "bytes".equals(schema.path("type").asText())
                && "org.apache.kafka.connect.data.Decimal".equals(schema.path("name").asText());
    }

    private int decimalScale(JsonNode schema) {
        String scale = schema.path("parameters").path("scale").asText();
        if (scale == null || scale.isBlank()) {
            return 0;
        }
        try {
            return Integer.parseInt(scale);
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    private String decodeDecimal(String base64, int scale) {
        try {
            byte[] raw = Base64.getDecoder().decode(base64);
            BigInteger unscaled = new BigInteger(raw);
            return new BigDecimal(unscaled, scale).toPlainString();
        } catch (Exception ignored) {
            return base64;
        }
    }

    private JsonNode fieldSchema(JsonNode structSchema, String fieldName) {
        if (structSchema == null || structSchema.isNull()) {
            return null;
        }
        JsonNode fields = structSchema.get("fields");
        if (fields == null || !fields.isArray()) {
            return null;
        }
        for (JsonNode field : fields) {
            if (fieldName.equals(field.path("field").asText())) {
                return field;
            }
        }
        return null;
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
