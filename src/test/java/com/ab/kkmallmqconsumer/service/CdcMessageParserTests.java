package com.ab.kkmallmqconsumer.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CdcMessageParserTests {

    private final CdcMessageParser parser = new CdcMessageParser(new com.fasterxml.jackson.databind.ObjectMapper());

    @Test
    void shouldParseBasicDebeziumEvent() {
        String payload = """
                {
                  \"before\": null,
                  \"after\": {\"id\": 1, \"productName\": \"iPhone\", \"updateTime\": \"2026-03-25T10:00:00\"},
                  \"op\": \"c\",
                  \"ts_ms\": 1742868000000,
                  \"source\": {\"db\": \"kkmall\", \"table\": \"mall_product\"}
                }
                """;

        var event = parser.parse(payload);
        assertEquals("c", event.getOp());
        assertEquals("kkmall", event.getDb());
        assertEquals("mall_product", event.getTable());
        assertNotNull(event.getAfter());
        assertEquals(1, ((Number) event.getAfter().get("id")).intValue());
    }

    @Test
    void shouldParseDebeziumEnvelopePayloadEvent() {
        String payload = """
                {
                  "schema": {
                    "type": "struct"
                  },
                  "payload": {
                    "before": {
                      "id": 804,
                      "product_name": "test",
                      "stock": 22
                    },
                    "after": {
                      "id": 804,
                      "product_name": "test",
                      "stock": 223,
                      "price": "AQ=="
                    },
                    "source": {
                      "db": "kkmall-dev",
                      "table": "mall_product"
                    },
                    "op": "u",
                    "ts_ms": 1774885944415
                  }
                }
                """;

        var event = parser.parse(payload);

        assertEquals("u", event.getOp());
        assertEquals("kkmall-dev", event.getDb());
        assertEquals("mall_product", event.getTable());
        assertEquals(1774885944415L, event.getTsMs());
        assertNotNull(event.getBefore());
        assertNotNull(event.getAfter());
        assertEquals(804, ((Number) event.getAfter().get("id")).intValue());
        assertEquals(223, ((Number) event.getAfter().get("stock")).intValue());
        assertEquals("AQ==", event.getAfter().get("price"));
    }
}
