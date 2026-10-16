# Consumer groups

Un groupe partage les partitions : une partition n'est assignée qu'à un consumer du même groupe. Démarrez deux fois `consume`, puis produisez. Observez `Processing partition` et `Partitions revoked`. Avec quatre partitions, plus de quatre consumers dans un groupe laisse des instances sans partition. Un autre groupe lit indépendamment le même topic.
