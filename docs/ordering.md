# Event ordering

Per-user ordering can be useful for an application that maintains account state, such as applying a
disablement after an enablement. The current provider guarantees durable, at-least-once delivery;
it does not guarantee global or per-user ordering. This document describes a possible future mode,
not an implemented configuration option.

The outbox claims due rows with `SKIP LOCKED`. A retry can delay an older event while a later event
is published, and different Keycloak nodes or relay workers can publish concurrently. Even one relay
worker does not fix retry ordering or concurrent capture. Event timestamps describe capture, not a
database commit sequence. Sorting received messages by time cannot prove an earlier message will
not arrive later.

A reliable per-user mode would need coordination at three points:

1. **Capture:** assign a durable sequence to `(realm ID, affected user ID)` under a database lock in
   the same Keycloak transaction as the event. Coordinate lock order with account mutations to avoid
   deadlocks. Preserve sequence state across user deletion/recreation and define keys for events
   without an affected user. An admin actor and the user being changed are different identities.
2. **Publication:** publish only the earliest uncompleted sequence for a key. An older event waiting
   for retry must block later events for that user, while other users can continue. Crash recovery
   must retain the same sequence, ID and bytes. This requires schema, claim-query and envelope changes.
3. **Consumption:** serialize processing for the key, or transactionally check and advance its last
   applied sequence. Multiple workers, redeliveries, quarantine replay and late duplicates must not
   reverse application state. Filtering and intentional drops need an explicit way to resolve gaps.

The provider can establish publication order, but receiving applications own processing order.
JetStream's consumer sequence and acknowledgement controls cannot repair messages that were already
published out of account order. See the [NATS consumer model](https://docs.nats.io/learn/jetstream/pull-consumers).

This would be a useful opt-in feature for applications that need ordered state changes. It adds
per-user locking and makes a failed event block later events for that user. It also needs dedicated
concurrency, rollback, restart and replay tests before being advertised as a guarantee. Existing
applications should continue to deduplicate by event ID and reconcile current state with Keycloak
when arrival order could otherwise re-enable a disabled or deleted account. Users do not need to
modify this repository to implement their own NATS receivers.
