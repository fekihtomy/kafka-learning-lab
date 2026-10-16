# Kafka basics

Un broker Kafka conserve des records dans des topics. Lancez `docker compose up -d`, puis `topic-create`. Résultat : un broker local et le topic `orders`. Contrairement à une queue MQ classique, Kafka retient les records pendant une durée configurée : consommation et suppression ne sont pas la même opération.
