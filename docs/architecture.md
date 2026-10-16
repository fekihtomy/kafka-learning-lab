# Architecture

```mermaid
flowchart LR
 O[Java OrderCreatedEvent] --> J[Jackson JSON serializer] --> K[Kafka orders]
 K --> D[Jackson JSON deserializer] --> C[Java OrderCreatedEvent]
 K --> P0[partition 0]; K --> P1[partition 1]; K --> P2[partition 2]; K --> P3[partition 3]
 P0 & P1 & P2 & P3 --> G[consumer group] --> A[consumer instances]
```

Une seule application et une seule classe de configuration suffisent au MVP. Le producer/consumer manipulent l'API Kafka officielle; les serializers isolent Jackson de la logique métier.
