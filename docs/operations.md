# Operating the bridge

## Production configuration

Use PostgreSQL with durable commits and a recovery/backup plan, plus a NATS cluster with at least three stream replicas on independent durable storage. Use `sync_interval: always` where power-loss durability is required. Do not infer that a stream replica count proves independent disks or failure domains. Test quorum loss, restore procedures and capacity under your own infrastructure; the local integration suite does not emulate loss of a whole three-node production cluster.

Use authenticated TLS and realm-appropriate NATS permissions. The Keycloak publisher needs publish access to its event subjects and the configured stream-info API, and subscribe access to its request reply inbox. It needs no stream/consumer create, update, purge or delete permission. Provision resources with a separate administrative identity. Consumers need their consumer-info, pull-next and acknowledgement subjects plus their reply inboxes. Do not allow unrelated publishers to reuse event IDs. Credentials and payloads are not intentionally logged by this provider.

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

The outbox is intentionally unbounded: an extended outage must consume database capacity or reject new operations. When capture cannot be persisted, the request fails rather than committing a missing event. Size the database for the expected outage window, measure row/index overhead, and use alerts before free space runs out. Adjust `KND_MAX_PAYLOAD_BYTES` and the broker/stream maximum message size together, allowing room for headers.

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
