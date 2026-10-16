# Replay

`replay` crée un group ID unique et démarre à `earliest`; il relit les records encore retenus. Produisez, consommez, puis exécutez `replay`. Résultat : les anciens événements réapparaissent, sans perturber `order-processing-group`. MQ Series → Kafka : le replay repose sur les offsets par groupe et la retention du journal, pas sur la remise de messages dans une queue.
