# Consumers

`consume` appelle `poll`, affiche le `ConsumerRecord`, traite puis commit en mode manuel. Lancez-le avant de produire. Le consumer ne retire pas le record du topic : il maintient sa position pour son groupe. Ctrl+C provoque `wakeup`, fermeture du client et libération du groupe.
