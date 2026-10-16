# Topics and partitions

Un topic est découpé en partitions, ici quatre. Lancez `topic-info` puis `produce --count 20` : la partition est affichée. Les partitions apportent du parallélisme, mais l'ordre Kafka n'existe qu'à l'intérieur d'une partition. MQ Series → Kafka : ne transposez pas directement une queue à une partition ; un topic Kafka est un journal partitionné.
