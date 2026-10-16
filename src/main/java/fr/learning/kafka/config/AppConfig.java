package fr.learning.kafka.config;

import java.util.Map;

/** Centralises environment-driven settings so experiments need no code changes. */
public record AppConfig(String bootstrapServers, String ordersTopic, String consumerGroup,
                        String autoOffsetReset, boolean autoCommit, int partitions) {
    public static AppConfig fromEnvironment() {
        Map<String, String> e = System.getenv();
        return new AppConfig(value(e, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                value(e, "KAFKA_TOPIC_ORDERS", "orders"), value(e, "KAFKA_CONSUMER_GROUP", "order-processing-group"),
                value(e, "KAFKA_AUTO_OFFSET_RESET", "earliest"), Boolean.parseBoolean(value(e, "KAFKA_ENABLE_AUTO_COMMIT", "false")),
                Integer.parseInt(value(e, "KAFKA_ORDERS_PARTITIONS", "4")));
    }
    private static String value(Map<String, String> values, String name, String fallback) { return values.getOrDefault(name, fallback); }
}
