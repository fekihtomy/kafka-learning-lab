package fr.learning.kafka.consumer;

import fr.learning.kafka.config.AppConfig;
import fr.learning.kafka.domain.OrderCreatedEvent;
import fr.learning.kafka.serialization.OrderEventDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public final class OrderConsumer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(OrderConsumer.class);
    private final KafkaConsumer<String, OrderCreatedEvent> consumer; private final AppConfig config; private final AtomicBoolean running = new AtomicBoolean(true);
    public OrderConsumer(AppConfig c, String groupId, String offsetReset) {
        config = c; Properties p = new Properties(); p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, c.bootstrapServers()); p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset); p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, c.autoCommit()); p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class); p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderEventDeserializer.class); p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); consumer = new KafkaConsumer<>(p);
    }
    public void run(String groupId) {
        LOG.info("Consumer instance started | Group ID: {} | auto-commit: {}", groupId, config.autoCommit());
        consumer.subscribe(java.util.List.of(config.ordersTopic()), new ConsumerRebalanceListener() {
            @Override public void onPartitionsAssigned(Collection<TopicPartition> ps) { ps.forEach(p -> LOG.info("Processing partition {}", p.partition())); }
            @Override public void onPartitionsRevoked(Collection<TopicPartition> ps) { LOG.info("Partitions revoked: {}", ps); }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "consumer-shutdown"));
        try {
            while (running.get()) {
                ConsumerRecords<String, OrderCreatedEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, OrderCreatedEvent> r : records) display(r);
                if (!config.autoCommit() && !records.isEmpty()) consumer.commitSync(); // process first, then commit
            }
        } catch (WakeupException ignored) { LOG.info("Consumer wakeup received"); }
        catch (Exception e) { LOG.error("Consumer stopped after an error", e); }
        finally { close(); LOG.info("Consumer shutdown complete"); }
    }
    private static void display(ConsumerRecord<String, OrderCreatedEvent> r) {
        OrderCreatedEvent e = r.value(); StringBuilder hs = new StringBuilder(); for (Header h : r.headers()) hs.append(h.key()).append('=').append(new String(h.value(), StandardCharsets.UTF_8)).append(' ');
        System.out.printf("%nMESSAGE RECEIVED%n topic     : %s%n partition : %d%n offset    : %d%n timestamp : %d%n key       : %s%n headers   : %s%n eventId   : %s%n orderId   : %s%n customer  : %s%n amount    : %s %s%n", r.topic(), r.partition(), r.offset(), r.timestamp(), r.key(), hs, e.eventId(), e.orderId(), e.customerId(), e.amount(), e.currency());
    }
    public void stop() { running.set(false); consumer.wakeup(); }
    @Override public void close() { consumer.close(Duration.ofSeconds(5)); }
}
