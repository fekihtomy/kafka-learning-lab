package fr.learning.kafka.cli;

import fr.learning.kafka.config.AppConfig;
import fr.learning.kafka.consumer.OrderConsumer;
import fr.learning.kafka.domain.OrderCreatedEvent;
import fr.learning.kafka.kafka.TopicAdmin;
import fr.learning.kafka.producer.OrderProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.Scanner;

/** Deliberately small CLI: direct Kafka client API is visible in the source. */
public final class KafkaLearningLab {
    public static void main(String[] args) {
        try { dispatch(args, AppConfig.fromEnvironment()); } catch (Exception e) { System.err.println("Error: " + e.getMessage()); System.exit(1); }
    }
    static void dispatch(String[] args, AppConfig c) throws Exception {
        if (args.length == 0) { help(); return; }
        switch (args[0]) {
            case "topic-create" -> { try (var a = new TopicAdmin(c)) { a.createIfMissing(c.ordersTopic(), c.partitions()); System.out.printf("Topic %s ready with %d partitions%n", c.ordersTopic(), c.partitions()); } }
            case "topic-info" -> { try (var a = new TopicAdmin(c)) { var d = a.describe(c.ordersTopic()); System.out.printf("Topic %s: %d partitions%n%s%n", d.name(), d.partitions().size(), d.partitions()); } }
            case "produce" -> produce(args, c);
            case "consume" -> new OrderConsumer(c, c.consumerGroup(), c.autoOffsetReset()).run(c.consumerGroup());
            case "replay" -> { String group = "replay-orders-" + Instant.now().toEpochMilli(); new OrderConsumer(c, group, "earliest").run(group); }
            case "config" -> System.out.println(c);
            default -> help();
        }
    }
    private static void produce(String[] args, AppConfig c) throws Exception {
        int count = optionInt(args, "--count", 1); boolean keyed = !has(args, "--without-key");
        try (var producer = new OrderProducer(c)) {
            if (has(args, "--interactive")) { try (Scanner in = new Scanner(System.in)) { System.out.print("customerId: "); String customer = in.nextLine(); System.out.print("orderId: "); String order = in.nextLine(); System.out.print("amount: "); BigDecimal amount = new BigDecimal(in.nextLine()); System.out.print("currency: "); send(producer, new OrderCreatedEvent("evt-" + UUID.randomUUID(), "ORDER_CREATED", Instant.now(), order, customer, amount, in.nextLine()), keyed); } return; }
            for (int i = 1; i <= count; i++) { String suffix = String.format("%04d", i); send(producer, new OrderCreatedEvent("evt-" + UUID.randomUUID(), "ORDER_CREATED", Instant.now(), "ORD-" + suffix, "CUS-" + (i % 10), BigDecimal.valueOf(10L + i), "EUR"), keyed); }
        }
    }
    private static void send(OrderProducer producer, OrderCreatedEvent e, boolean keyed) throws Exception { RecordMetadata m = producer.send(e, keyed); System.out.printf("Order sent successfully%n eventId : %s%n orderId : %s%n customerId : %s%n partition : %d%n offset : %d%n timestamp : %d%n%n", e.eventId(), e.orderId(), e.customerId(), m.partition(), m.offset(), m.timestamp()); }
    private static boolean has(String[] a, String x) { for (String s : a) if (s.equals(x)) return true; return false; }
    private static int optionInt(String[] a, String key, int fallback) { for (int i=0;i<a.length-1;i++) if(a[i].equals(key)) return Integer.parseInt(a[i+1]); return fallback; }
    private static void help() { System.out.println("Kafka Learning Lab\nCommands: topic-create | topic-info | produce [--count N] [--interactive] [--without-key] | consume | replay | config"); }
}
