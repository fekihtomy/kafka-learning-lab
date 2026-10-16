# JSON serialization

Le record Java est immuable; `OrderEventSerializer` utilise Jackson pour produire les bytes Kafka, puis `OrderEventDeserializer` recompose l'objet. Lancez `mvn test` pour le round-trip Java → JSON → Java. JSON est lisible et utile pour l'apprentissage, mais un schéma typé (hors périmètre) sera préférable dans des usages plus stricts.
