# Offsets

L'offset est la position croissante d'un record **dans une partition**. Produisez puis observez les offsets : deux partitions peuvent toutes deux avoir l'offset 5; ce n'est pas un identifiant global. Le commit mémorise la prochaine position à lire pour le groupe. Avec `KAFKA_ENABLE_AUTO_COMMIT=true`, Kafka commet périodiquement : c'est plus simple, mais le moment du commit est moins contrôlé.
