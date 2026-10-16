package fr.learning.kafka.domain;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal; import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;
class OrderCreatedEventTest {
 @Test void createsValidEvent() { var e = event(); assertEquals("CUS-1", e.customerId()); }
 @Test void rejectsNonPositiveAmount() { assertThrows(IllegalArgumentException.class, () -> new OrderCreatedEvent("e","ORDER_CREATED",Instant.now(),"o","c",BigDecimal.ZERO,"EUR")); }
 @Test void rejectsUnknownType() { assertThrows(IllegalArgumentException.class, () -> new OrderCreatedEvent("e","OTHER",Instant.now(),"o","c",BigDecimal.ONE,"EUR")); }
 static OrderCreatedEvent event() { return new OrderCreatedEvent("evt-1","ORDER_CREATED",Instant.parse("2026-10-08T10:30:00Z"),"ORD-1","CUS-1",new BigDecimal("149.90"),"EUR"); }
}
