# Headers and metadata

Le producer ajoute `eventType`, `source` et `version`. Le consumer les affiche avec topic, partition, offset, timestamp et key. Produisez un record et inspectez la sortie. Les headers transportent un contexte léger hors du JSON; ils ne remplacent ni validation de payload ni modèle de sécurité.
