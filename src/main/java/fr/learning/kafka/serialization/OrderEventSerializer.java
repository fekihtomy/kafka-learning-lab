package fr.learning.kafka.serialization;

import fr.learning.kafka.domain.OrderCreatedEvent;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

public final class OrderEventSerializer implements Serializer<OrderCreatedEvent> {
    @Override public byte[] serialize(String topic, OrderCreatedEvent value) {
        if (value == null) return null;
        try { return Json.MAPPER.writeValueAsBytes(value); }
        catch (Exception e) { throw new IllegalArgumentException("Cannot serialize OrderCreatedEvent", e); }
    }
    @Override public void configure(Map<String, ?> configs, boolean isKey) { }
}
