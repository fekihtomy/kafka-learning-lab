package fr.learning.kafka.producer;

import fr.learning.kafka.config.AppConfig;
import fr.learning.kafka.domain.OrderCreatedEvent;
import fr.learning.kafka.serialization.OrderEventSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

public final class OrderProducer implements AutoCloseable {
    private final KafkaProducer<String, OrderCreatedEvent> producer; private final String topic;
    public OrderProducer(AppConfig config) { this.topic = config.ordersTopic(); Properties p = new Properties(); p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers()); p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderEventSerializer.class); p.put(ProducerConfig.ACKS_CONFIG, "all"); p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15000); this.producer = new KafkaProducer<>(p); }
    public RecordMetadata send(OrderCreatedEvent event, boolean keyed) throws Exception {
        RecordHeaders headers = new RecordHeaders(); headers.add("eventType", "ORDER_CREATED".getBytes(StandardCharsets.UTF_8)); headers.add("source", "kafka-learning-lab".getBytes(StandardCharsets.UTF_8)); headers.add("version", "1".getBytes(StandardCharsets.UTF_8));
        return producer.send(new ProducerRecord<>(topic, null, null, keyed ? event.customerId() : null, event, headers)).get();
    }
    @Override public void close() { producer.close(Duration.ofSeconds(5)); }
}
