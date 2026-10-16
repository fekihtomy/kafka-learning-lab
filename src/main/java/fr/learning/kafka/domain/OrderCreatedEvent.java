package fr.learning.kafka.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

public record OrderCreatedEvent(String eventId, String eventType, Instant timestamp, String orderId,
                                String customerId, BigDecimal amount, String currency) {
    public OrderCreatedEvent {
        required(eventId, "eventId"); required(eventType, "eventType"); required(timestamp, "timestamp");
        required(orderId, "orderId"); required(customerId, "customerId"); required(amount, "amount"); required(currency, "currency");
        if (!"ORDER_CREATED".equals(eventType)) throw new IllegalArgumentException("eventType must be ORDER_CREATED");
        if (amount.signum() <= 0) throw new IllegalArgumentException("amount must be positive");
    }
    private static void required(Object value, String name) { if (value == null || (value instanceof String s && s.isBlank())) throw new IllegalArgumentException(name + " is required"); }
}
