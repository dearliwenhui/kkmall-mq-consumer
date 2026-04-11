package com.ab.kkmallmqconsumer.model;

import java.time.LocalDateTime;

public record OrderTimeoutCheckMessage(Long orderId, String orderNo, Long userId, LocalDateTime expireTime) {
}
