# Database initialization and space management

The extension already initializes its schema through `OutboxEntityProviderFactory` and the packaged [Liquibase changelog](../extension/src/main/resources/META-INF/nats-outbox-changelog.xml). Keycloak runs these versioned migrations during startup before starting the relay. This handles both fresh databases and existing Keycloak installations. An `initdb` folder is unnecessary for the outbox and would create a second schema owner: PostgreSQL container initialization scripts run only against empty database directories and do not handle later upgrades. The demo's `deploy/postgres-init.sql` provisions the separate consumer database/user, not extension tables.

Keep the same changelog path, factory ID and existing changeset IDs. Add new changesets for upgrades; never rewrite an already applied migration. Migration errors stop startup. The database identity running migration needs DDL rights, or operators must use an approved schema migration process before starting restricted runtime identities. Back up and test the supported Keycloak upgrade procedure first. No trigger or modification of a Keycloak core table is installed by the extension.

## Outbox lifecycle

Only selected events occupy the outbox. Payload size is bounded by `KND_MAX_PAYLOAD_BYTES`, representations and arbitrary event details are omitted, and each row contains one original payload plus bounded retry metadata. Retry updates cannot rewrite payload, subject or capture time. The due-time index supports ordered claims without scanning the entire backlog.

Confirmed publication deletes the row in the same transaction holding its lock. There is no delivered-event archive or periodic sweep to wait for. Failed publication, an uncertain ACK, interrupted processing or failed database commit retains the row. Realm/user deletion never cascades to pending events. Filtering out a type later never deletes its existing rows.

There is deliberately no age-based trim, retry-count deletion or automatic dead-letter purge. A durable queue cannot simultaneously accept unlimited new work during an unlimited outage and occupy bounded disk. Budget capacity for accepted event rate × outage duration × measured row/index/WAL overhead, monitor growth, and stop admitting relevant writes before storage is exhausted if capacity cannot be expanded. A database rejection fails the corresponding Keycloak transaction instead of losing the event.

## Reclaiming space

The second PostgreSQL changeset tunes autovacuum on the outbox and its TOAST table: vacuum scale factor 0.02, vacuum threshold 50; the main table's analyze scale factor is 0.05 with threshold 50. These are per-table settings and do not alter global database policy. Autovacuum must remain enabled and have enough I/O/worker capacity. Monitor long transactions and replication slots that prevent cleanup.

The maintenance migration obtains the configured schema and quotes identifiers through Liquibase, so it targets the extension table even outside `public` or when a schema name needs quoting. Use the deployment's schema/search path when running the monitoring queries below.

PostgreSQL DELETE makes tuples eligible for vacuum; ordinary vacuum generally makes space reusable inside the relation rather than immediately returning its entire high-water allocation to the filesystem. A stable nonzero relation size after draining is expected. Avoid a routine `VACUUM FULL`, which rewrites and locks the table; schedule exceptional compaction with the DBA after a large outage if returning disk is required. See [PostgreSQL vacuum behavior](https://www.postgresql.org/docs/17/routine-vacuuming.html).

```sql
SELECT pg_size_pretty(pg_total_relation_size('kc_nats_outbox')) AS table_toast_and_indexes,
       pg_size_pretty(pg_relation_size('kc_nats_outbox')) AS heap;

SELECT n_live_tup, n_dead_tup, last_autovacuum, last_autoanalyze
FROM pg_stat_user_tables WHERE relname = 'kc_nats_outbox';

SELECT reloptions FROM pg_class WHERE oid = 'kc_nats_outbox'::regclass;
```

Combine these with pending count, oldest event age and free-space monitoring in [operations](operations.md). Statistics are estimates; a zero pending count proves only that the outbox drained, not that consumers finished.

## Consumer retention

The example consumer's inbox stores event ID and payload hash for permanent deduplication; its effect ledger is application data. Deleting inbox rows after a fixed number of days breaks the once-only database-effect guarantee if an older message is redelivered or replayed. Prune only after proving that the replay horizon is closed across every broker, backlog, archive and restore process, or retain a compact permanent business idempotency key. JetStream's short deduplication window is insufficient evidence for pruning the consumer inbox.
