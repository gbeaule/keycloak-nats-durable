# Architecture and source review

## Why an outbox

A request-time NATS publish cannot atomically commit both PostgreSQL and JetStream. Publishing before the database commits can emit an event for an operation that later rolls back. An in-memory after-commit queue can lose an event if the process dies after database commit. Saving the event in the same transaction as the account change closes that gap without a distributed transaction.

Keycloak explicitly documents that JPA listener work joins its transaction in the [EventListenerProvider contract](https://www.keycloak.org/docs-api/26.7.4/javadocs/org/keycloak/events/EventListenerProvider.html). Its [custom entity API](https://www.keycloak.org/docs/latest/server_development/index.html#_extensions_jpa) registers the outbox entity and Liquibase migration in Keycloak's existing persistence unit. This documented registration mechanism is the accepted architecture. Keycloak gives it no compatibility guarantee, so the [compatibility policy](compatibility.md) defines runtime and upgrade tests for each supported release.

The [26.7.4 EventBuilder](https://github.com/keycloak/keycloak/blob/26.7.4/server-spi-private/src/main/java/org/keycloak/events/EventBuilder.java) and [AdminEventBuilder](https://github.com/keycloak/keycloak/blob/26.7.4/services/src/main/java/org/keycloak/services/resources/admin/AdminEventBuilder.java) catch listener exceptions. The provider therefore calls `setRollbackOnly()` on any capture/serialization failure. Flush/commit errors roll back through JPA. Error events can run in a distinct transaction; the extension honors the transaction in which Keycloak invokes it.

## Reviewed projects

* [ebbot-ai/keycloak-nats](https://github.com/ebbot-ai/keycloak-nats): reviewed `NATSEventListenerProvider` and its factory. It demonstrates subject construction and direct JetStream publishing. Its reviewed implementation logs publish failures and uses a no-op listener when initial connection fails. It does not provide a transactional outbox or preserve failed publishes across restart. This project uses an independent implementation.
* [p2-inc/keycloak-events](https://github.com/p2-inc/keycloak-events): reviewed `SenderEventListenerProvider`, the listener base, and the documented user lifecycle mechanism. Its sender uses scheduled in-memory retry tasks. The user-removal provider mechanism also highlights that admin events do not cover every possible storage-provider change. We adopt neither its code nor its runtime dependency; event coverage is stated explicitly.
* [cevheri/keycloak-custom-event-listener](listener-comparison.md): reviewed the listener, HTTP client and `initdb` script at a pinned commit. Synchronous webhook delivery with logged failures does not preserve unsent events across restart or coordinate delivery with database commit. Its smaller persistence API footprint does not provide equivalent durable guarantees.

The review informed the failure model and tests. This project does not copy source or depend on these projects.

## Publisher coordination

Every Keycloak node runs one relay after schema migration. Each attempt uses a fresh Keycloak session and transaction, selects one due row using PostgreSQL `FOR UPDATE SKIP LOCKED` via Hibernate, waits for a bounded JetStream publish acknowledgement, then deletes and commits. Other nodes skip locked rows. There is no shared `EntityManager`, global scheduler lock, lease extension, or in-memory source of truth.

`OutboxRepository` contains JPQL over the mapped entity and uses Keycloak's managed connection. The hint name is `SpecHints.HINT_SPEC_LOCK_TIMEOUT`; its value is Hibernate's [`Timeouts.SKIP_LOCKED_MILLI`](https://docs.hibernate.org/orm/7.2/javadocs/org/hibernate/Timeouts.html#SKIP_LOCKED_MILLI). That symbolic sentinel means “skip a row already locked by another transaction,” not a negative wait duration. No Hibernate magic number is embedded in the query. The integration suite holds a due row locked while another row is delivered to test this behavior directly.

## Wakeups and polling

After a successful local database commit, the listener sets a pending wakeup flag under a monitor. The worker consumes this flag only when it reaches its wait, under the same monitor, so a commit between scanning and sleeping cannot lose its notification. Repeated signals coalesce into one pending flag. There is no accumulating permit count or lifetime generation counter. Multiple events in one transaction register one wakeup. Rollbacks do not signal, and the callback performs no database or network work.

Full batches drain immediately. Otherwise idle scans back off to `KND_IDLE_POLL_MAX_MS` (five seconds by default), borrowing a database connection only for the scan. A local commit wakes the worker regardless of that delay. On a quiet cluster of N nodes, steady-state fallback traffic is approximately N/5 indexed selection queries per second, plus the transaction/session overhead. This is an estimate, not a load-test measurement. Retry eligibility is a minimum time; scheduling and outage recovery can add up to the idle scan interval.

Polling is still necessary as recovery insurance: another node can die after commit and before its wakeup, and in-memory notifications do not survive a restart. PostgreSQL LISTEN/NOTIFY can reduce cross-node wakeup latency but still needs catch-up scans, a persistent listener connection and DB-specific notification setup. CDC can replace scans with a transaction-log reader at high volume, but adds operational infrastructure and retention/checkpoint responsibilities. The current hybrid keeps installation to a provider JAR and the existing database; benchmark before adding another service.

Holding a transaction across network requests is deliberate: it avoids lease-expiry races and ownership complexity. It consumes one database connection per active node and limits throughput. Keep timeouts short, monitor database pools, and benchmark under expected load. Large installations can replace the relay with a leased or CDC publisher while preserving the persisted message identity; that is outside this implementation.

Crash after acceptance but before delete/commit is unavoidable without a distributed transaction. Every retry reuses the persisted ID and bytes. Stream configuration is checked for each attempt rather than only at startup, so changes to retention settings stop publishing until corrected. This cannot prevent a privileged operator from changing or deleting stored data after a successful publish: stream administration must be restricted operationally.

## Shutdown and long-running processes

Shutdown stops further relay work, closes the wakeup, interrupts the worker and closes the NATS connection. The relay checks shutdown again after the database lookup, before publication. A publication already in progress may complete; a missing or uncertain acknowledgement retains its row for retry.

The publisher serializes publication separately from connection lifecycle changes. Closing it can cancel an in-flight request without waiting for the publication monitor. If an initial connection finishes after shutdown, the publisher immediately closes that connection instead of installing or leaking it. The factory never holds its startup/shutdown monitor while closing sockets or waiting for the worker. Connection and request timeouts still apply; the five-second termination wait reports a warning if cleanup has not finished.

The persisted attempt count saturates at `Long.MAX_VALUE`; retry scheduling continues after saturation. Backoff arithmetic and batch work are bounded by validated configuration. Idle waits use elapsed `System.nanoTime()` differences, so a signed clock wrap is safe for their bounded duration. No growing counter is used for notification identity or ownership. Regression tests force shutdown interleavings with latches and place retry state directly at the numeric boundary.

## Delivery and retention

[JetStream's model](https://github.com/nats-io/nats.docs/blob/master/using-nats/jetstream/model_deep_dive.md) describes message-ID deduplication, explicit acknowledgements and confirmed acknowledgements. Deduplication has a finite window. A consumer can also crash after a side effect before acknowledging. A database inbox with a unique `(consumer_name, event_id)` key and the effect in the same transaction addresses both cases.

WorkQueue retention matches one logical handler with competing workers. It retains messages even before that handler exists and removes them on ACK. Limits retention can support multiple independent handlers, but needs an explicit history-pruning policy. Interest retention is rejected because publications can disappear when no consumer exists. All accepted configurations require File storage, DiscardNew, no age expiry, no per-subject cap, no TTL/rollup, and the configured replica minimum.

The official [JetStream durability guidance](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/README.md) also discusses the disk synchronization interval. The example and test servers use `sync_interval: always`; production must also configure durable replicated storage and evaluate the associated throughput cost. A publisher cannot verify host disks, power-loss behavior or all replica placement through stream metadata.

## Boundaries

No sequence in the payload is advertised as a commit-order sequence. Event timestamps do not resolve races across concurrent operations. User enablement is a state observation, not evidence of an enablement transition. Existing event types remain Keycloak types; the extension does not invent a reliable global `USER_DISABLED` transition from a generic update event.

Events for external systems that do not participate in the Keycloak JPA transaction need their own durable capture integration. This provider does not intercept every `UserStorageProvider`, direct SQL change, LDAP synchronization or arbitrary custom provider action. It also does not backfill events that occurred before installation or while a realm's listener was disabled.
