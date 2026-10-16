# Producers

Le producer publie un `OrderCreatedEvent` et reçoit un metadata contenant partition et offset. Exécutez `produce --count 10`. Chaque confirmation prouve que le broker a accepté le record (`acks=all`). Les erreurs de disponibilité ou délai remontent proprement au CLI.
