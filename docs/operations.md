# Operating the bridge

## Production configuration

Use PostgreSQL with durable commits and a recovery/backup plan, plus a NATS cluster with at least three stream replicas on independent durable storage. Use `sync_interval: always` where power-loss durability is required. Do not infer that a stream replica count proves independent disks or failure domains. The integration suite exercises a three-node NATS cluster with mutual TLS, leader failure and majority loss; restore procedures, database failover and capacity still need testing in the target infrastructure. See [cluster and certificate configuration](configuration.md).

Keep PostgreSQL `fsync` enabled and `synchronous_commit` set for durable commits; synchronous replication settings must also match the desired database failover guarantee. Keycloak has an [asynchronous-commit optimization for ephemeral data](https://www.keycloak.org/server/db#_asynchronous_commits). The outbox entity does not opt into it, and an integration test checks the effective setting at commit using a deferred trigger. Do not change the entity to allow asynchronous commits. Disabling the optimization with `--spi-connections-jpa--quarkus--async-commit=false` is an additional deployment option; it does not fix an unsafe server/database default of `synchronous_commit=off`.

TLS is optional: choose `nats://` for plaintext or `tls://` for verified TLS. Use TLS when the deployment requires transport encryption, and configure realm-appropriate NATS permissions independently. The Keycloak publisher needs publish access to its event subjects and the configured stream-info API, and subscribe access to its request reply inbox. It needs no stream/consumer create, update, purge or delete permission. Provision resources with a separate administrative identity. Consumers need their consumer-info, pull-next and acknowledgement subjects plus their reply inboxes. Do not allow unrelated publishers to reuse event IDs. Credentials and payloads are not intentionally logged by this provider.

The provided sample consumer's automatic DDL is convenient for the demo; use a migration identity and provision its tables separately when production identities cannot create tables. Each logical consumer must share its inbox across worker replicas. Do not clear inbox rows as a restart procedure.

## Monitoring

Poll the outbox independently of Keycloak readiness. Alert on queue growth, oldest event age, repeated failures, database free space, NATS storage free space, lost quorum, disconnected clients, consumer pending messages and redeliveries. Store metrics in your monitoring system and set thresholds from the application's delivery latency requirement.

```sql
SELECT count(*) AS pending,
       COALESCE(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 - min(created_at), 0) AS oldest_age_ms,
       max(attempts) AS largest_retry_count
FROM kc_nats_outbox;

SELECT last_error, count(*) AS pending, max(attempts) AS largest_retry_count
FROM kc_nats_outbox GROUP BY last_error;

SELECT id, created_at, attempts, next_attempt_at, last_error
FROM kc_nats_outbox ORDER BY created_at LIMIT 100;
```

`LAST_ERROR` contains a failure category, not raw server text or event contents. Publish failures log on attempt 1 and powers of two. Transaction failures log separately and leave data recoverable. An empty outbox alone does not mean consumers are caught up; inspect stream and durable-consumer state too. The example exposes NATS monitoring on loopback port 8222; production should restrict it to the monitoring network.

The `io.github.gbeaule.keycloaknats.NatsDiagnostics` logger reports connection transitions, authentication/permission failures, exceptions, slow consumers, discarded messages and socket write timeouts. Relay warnings include the retry time, event ID, controlled stream-validation reason or JetStream numeric status/API code. Exception cause categories are retained; raw remote exception text, URLs and payloads are not logged because they can contain credentials or injected lines. For a generic `server_error`, correlate with NATS server logs. An unsafe-stream warning now explains the failed rule (for example `DiscardNew is required`).

During sustained idle periods the scan delay grows to `KND_IDLE_POLL_MAX_MS`; a local commit wakes the relay sooner. Full batches drain without an extra polling delay. Lower the idle maximum if cross-node crash recovery needs tighter latency; tune it with database load measurements. A database connection is held during each synchronous publish attempt, not while the worker sleeps.

The outbox is intentionally unbounded: an extended outage must consume database capacity or reject new operations. When capture cannot be persisted, the request fails rather than committing a missing event. Size the database for the expected outage window, measure row/index overhead, and use alerts before free space runs out. Adjust `KND_MAX_PAYLOAD_BYTES` and the broker/stream maximum message size together, allowing room for headers.

Use a [capture policy](configuration.md#choose-events-before-storing-them) to avoid storing unwanted events. Confirmed publications are removed immediately; retries update only bounded metadata. The migration tunes PostgreSQL autovacuum for the outbox and TOAST storage. Monitor dead tuples, relation sizes and blocked vacuum alongside free space; see [database lifecycle and retention](database.md). Pending rows and the consumer inbox must not be trimmed by age as a capacity workaround.

## Recovery

| Symptom | Action |
|---|---|
| NATS absent at boot or disconnected | Restore NATS/network/authentication; the relay retries automatically. Keycloak can start and accumulate events. |
| Unsafe/missing stream | Provision or repair the intended stream. Verify File storage, replica count, exact subject prefix, DiscardNew, no expiry, supported retention and publish ACKs. No backlog rows need to be recreated. |
| Stream full | Restore/scale consumers or increase capacity. WorkQueue ACKs free space. Do not purge unprocessed messages. |
| Poison event or schema mismatch | Fix the consumer/publisher configuration and retry the same ID. Events are not silently discarded after N attempts. |
| Consumer has reached a configured MaxDeliver | This is a misconfiguration for indefinite retries; repair it and arrange redelivery. The shipped consumer uses unlimited MaxDeliver. |
| Keycloak process dies during publish | Restart any correctly configured node against the same database. Lock recovery and the saved ID handle retry. |
| Inbox ID has different payload | Investigate publisher identity reuse or data corruption. The consumer refuses to treat different content as a processed duplicate. |
| Database unavailable | Restore it; Keycloak account changes cannot safely proceed without the shared transaction. |

There is no automatic dead-letter sink. If the application needs one, durably write the original event and reason to a quarantine store before acknowledging, monitor that store, and provide a replay process that retains the event ID. An ACK, TERM, purge, stream/consumer deletion, TTL or manual outbox deletion can abandon work; do not use those as routine error handling.

Restoring the Keycloak database to an older snapshot may replay published events; restoring a consumer database can also remove inbox entries. Coordinate backup recovery across the pipeline and downstream effects. The system cannot undo data lost by restoring durable stores to mutually inconsistent points in time.

## Deployment and upgrades

Keep the JAR and configuration consistent across every Keycloak node. Liquibase adds its table on startup; it never cascades pending-event deletion with realm or user deletion. Test schema migrations on a backup, and drain pending data before any destructive downgrade or provider removal.

The stream prefix and name are part of the deployment contract. Existing outbox rows retain their original subject; changing these settings mid-backlog requires an explicit migration/replay plan. The simple relay has one thread per node and holds one database transaction while waiting for bounded broker requests. Increase node count or redesign the relay after a throughput benchmark, rather than increasing batch size and request timeout without measuring pool pressure.

The supplied demo uses a single broker and development Keycloak mode. Its credentials, network settings and one-replica override are not production defaults. For multiple independent consuming applications, explicitly provision a Limits stream and independent durable names, and prune only history processed by all required consumers.
