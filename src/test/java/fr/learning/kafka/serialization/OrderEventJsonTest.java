package fr.learning.kafka.serialization;
import fr.learning.kafka.domain.OrderCreatedEvent; import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class OrderEventJsonTest {
 @Test void roundTrip() { var original = new OrderCreatedEvent("evt-1","ORDER_CREATED",java.time.Instant.parse("2026-10-08T10:30:00Z"),"ORD-1","CUS-1",new java.math.BigDecimal("149.90"),"EUR"); var bytes = new OrderEventSerializer().serialize("orders",original); assertEquals(original,new OrderEventDeserializer().deserialize("orders",bytes)); }
 @Test void invalidJsonFailsClearly() { assertThrows(IllegalArgumentException.class, () -> new OrderEventDeserializer().deserialize("orders", "not json".getBytes())); }
}
