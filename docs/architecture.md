# Architecture and source review

## Why an outbox

A request-time NATS publish cannot atomically commit both PostgreSQL and JetStream. Publishing before the database commits can emit an event for an operation that later rolls back. An in-memory after-commit queue can lose an event if the process dies after database commit. Saving the event in the same transaction as the account change closes that gap without a distributed transaction.

Keycloak explicitly documents that JPA listener work joins its transaction in the [EventListenerProvider contract](https://www.keycloak.org/docs-api/26.7.4/javadocs/org/keycloak/events/EventListenerProvider.html). Its [custom entity SPI](https://www.keycloak.org/docs/latest/server_development/index.html#_extensions_jpa) supplies the shared entity manager and Liquibase migration. This is an unsupported SPI, so the exact Keycloak version is pinned and tested.

The [26.7.4 EventBuilder](https://github.com/keycloak/keycloak/blob/26.7.4/server-spi-private/src/main/java/org/keycloak/events/EventBuilder.java) and [AdminEventBuilder](https://github.com/keycloak/keycloak/blob/26.7.4/services/src/main/java/org/keycloak/services/resources/admin/AdminEventBuilder.java) catch listener exceptions. The provider therefore calls `setRollbackOnly()` on any capture/serialization failure. Flush/commit errors roll back through JPA. Error events can run in a distinct transaction; the extension honors the transaction in which Keycloak invokes it.

## Reviewed projects

* [ebbot-ai/keycloak-nats](https://github.com/ebbot-ai/keycloak-nats): reviewed `NATSEventListenerProvider` and its factory. It demonstrates subject construction and direct JetStream publishing. Its reviewed implementation logs publish failures and uses a no-op listener when initial connection fails. It does not provide a transactional outbox or preserve failed publishes across restart. This project uses an independent implementation.
* [p2-inc/keycloak-events](https://github.com/p2-inc/keycloak-events): reviewed `SenderEventListenerProvider`, the listener base, and the documented user lifecycle mechanism. Its sender uses scheduled in-memory retry tasks. The user-removal provider mechanism also highlights that admin events do not cover every possible storage-provider change. We adopt neither its code nor its runtime dependency; event coverage is stated explicitly.

The review informed the failure model and tests, rather than copying source or depending on either project.

## Publisher coordination

Every Keycloak node runs one relay after schema migration. Each attempt uses a fresh Keycloak session and transaction, selects one due row using PostgreSQL `FOR UPDATE SKIP LOCKED` via Hibernate, waits for a bounded JetStream publish acknowledgement, then deletes and commits. Other nodes skip locked rows. There is no shared `EntityManager`, global scheduler lock, lease extension, or in-memory source of truth.

Holding a transaction across network requests is deliberate: it avoids lease-expiry races and ownership complexity. It consumes one database connection per active node and limits throughput. Keep timeouts short, monitor database pools, and benchmark under expected load. Large installations can replace the relay with a leased or CDC publisher while preserving the persisted message identity; that is outside this implementation.

Crash after acceptance but before delete/commit is unavoidable without a distributed transaction. Every retry reuses the persisted ID and bytes. Stream configuration is checked for each attempt rather than only at startup, so changes to retention settings stop publishing until corrected. This cannot prevent a privileged operator from changing or deleting stored data after a successful publish: stream administration must be restricted operationally.

## Delivery and retention

[JetStream's model](https://github.com/nats-io/nats.docs/blob/master/using-nats/jetstream/model_deep_dive.md) describes message-ID deduplication, explicit acknowledgements and confirmed acknowledgements. Deduplication has a finite window. A consumer can also crash after a side effect before acknowledging. A database inbox with a unique `(consumer_name, event_id)` key and the effect in the same transaction addresses both cases.

WorkQueue retention matches one logical handler with competing workers. It retains messages even before that handler exists and removes them on ACK. Limits retention can support multiple independent handlers, but needs an explicit history-pruning policy. Interest retention is rejected because publications can disappear when no consumer exists. All accepted configurations require File storage, DiscardNew, no age expiry, no per-subject cap, no TTL/rollup, and the configured replica minimum.

The official [JetStream durability guidance](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/README.md) also discusses the disk synchronization interval. The example and test servers use `sync_interval: always`; production must also configure durable replicated storage and evaluate the associated throughput cost. A publisher cannot verify host disks, power-loss behavior or all replica placement through stream metadata.

## Boundaries

No sequence in the payload is advertised as a commit-order sequence. Event timestamps do not resolve races across concurrent operations. User enablement is a state observation, not evidence of an enablement transition. Existing event types remain Keycloak types; the extension does not invent a reliable global `USER_DISABLED` transition from a generic update event.

Events for external systems that do not participate in the Keycloak JPA transaction need their own durable capture integration. This provider does not intercept every `UserStorageProvider`, direct SQL change, LDAP synchronization or arbitrary custom provider action. It also does not backfill events that occurred before installation or while a realm's listener was disabled.
