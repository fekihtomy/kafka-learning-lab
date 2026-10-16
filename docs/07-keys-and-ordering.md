# Keys and ordering

Par défaut le producer utilise `customerId` comme key. Produisez plusieurs événements : un même client revient sur la même partition, donc son ordre est préservé dans cette partition. Comparez avec `produce --count 50 --without-key`. Une key ne donne pas un ordre global, et ajouter des partitions peut changer l'affectation future. MQ Series → Kafka : le parallélisme se paie par l'absence d'ordre inter-partitions.
