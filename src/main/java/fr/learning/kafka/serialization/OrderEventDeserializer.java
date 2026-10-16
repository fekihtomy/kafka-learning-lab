package fr.learning.kafka.serialization;

import fr.learning.kafka.domain.OrderCreatedEvent;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;

public final class OrderEventDeserializer implements Deserializer<OrderCreatedEvent> {
    @Override public OrderCreatedEvent deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try { return Json.MAPPER.readValue(data, OrderCreatedEvent.class); }
        catch (Exception e) { throw new IllegalArgumentException("Invalid JSON event on topic " + topic, e); }
    }
    @Override public void configure(Map<String, ?> configs, boolean isKey) { }
}
