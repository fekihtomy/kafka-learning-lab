package fr.learning.kafka;

import fr.learning.kafka.domain.OrderCreatedEvent;
import fr.learning.kafka.serialization.OrderEventDeserializer;
import fr.learning.kafka.serialization.OrderEventSerializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

/** A real KRaft broker; run explicitly with: mvn verify -Pintegration. */
@Testcontainers
class KafkaEndToEndIT {
    @Container static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.2"));
    @Test @Timeout(30) void producerThenConsumerRoundTrip() throws Exception {
        String topic = "orders-it"; String group = "it-group-" + System.nanoTime();
        Properties pp = new Properties(); pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()); pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderEventSerializer.class);
        var event = new OrderCreatedEvent("evt-it", "ORDER_CREATED", Instant.now(), "ORD-it", "CUS-it", BigDecimal.TEN, "EUR");
        try (var producer = new KafkaProducer<String, OrderCreatedEvent>(pp)) { producer.send(new ProducerRecord<>(topic, event.customerId(), event)).get(); }
        Properties cp = new Properties(); cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()); cp.put(ConsumerConfig.GROUP_ID_CONFIG, group); cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class); cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderEventDeserializer.class);
        try (var consumer = new KafkaConsumer<String, OrderCreatedEvent>(cp)) { consumer.subscribe(List.of(topic)); ConsumerRecords<String, OrderCreatedEvent> records = ConsumerRecords.empty(); for (int i = 0; i < 20 && records.isEmpty(); i++) records = consumer.poll(Duration.ofMillis(500)); assertEquals(1, records.count()); assertEquals(event, records.iterator().next().value()); }
    }
}
