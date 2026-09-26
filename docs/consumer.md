# Consumer deadlines, monitoring and recovery

This page describes the optional `consumer-example` application and its operator commands. The
Keycloak provider publishes standard NATS messages; users normally receive them in their own
applications with any compatible NATS client. They do not modify this repository or install its
example database as part of the provider. `recordEffect` writes a demo ledger to make duplicate
processing observable in tests. The deadlines and shedding settings below apply only to this
example application; independent receivers own their equivalent policies.

The example consumer commits its inbox entry and database effects in one transaction, then sends a
confirmed ACK. Each worker handles one delivery at a time. Run additional replicas with the same
durable name and shared application database for concurrency. They share the broker's MaxAckPending
window. Handlers must use the supplied JDBC transaction, must not commit it themselves, and must not
perform external HTTP, email or other effects that cannot roll back with that transaction.

## Schema ownership

Apply [consumer.sql](../consumer-example/src/main/resources/db/consumer.sql) using your migration
identity, or run the packaged migration command with that identity:

```sh
java -jar consumer-example/target/consumer-example-1.0.0-SNAPSHOT.jar migrate
```

Connection settings are `KND_CONSUMER_DB_URL`, `KND_CONSUMER_DB_USER` and
`KND_CONSUMER_DB_PASSWORD`. The `run` command (also the default) requires the schema to exist.
Automatic DDL is disabled by default. Compose explicitly enables `KND_CONSUMER_AUTO_MIGRATE=true`
for the demo. Production runtime permissions need SELECT/INSERT on the inbox, INSERT on application
effect tables, and SELECT/INSERT on quarantine. Replay additionally needs UPDATE on quarantine and
NATS publish permission. Provisioning and migration identities can be kept separate from workers.

For a restricted runtime role, also check privileges inherited through `PUBLIC`. PostgreSQL 14 and
databases upgraded from older defaults can grant everyone `CREATE` on the `public` schema. A database
owner can revoke that grant and grant schema creation only to the migration identity:

```sql
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
```

Apply this to the receiver's database with awareness of its other applications. The integration
fixture explicitly establishes this restriction on every tested major before checking that the runtime
role can process/quarantine messages but cannot create tables. See the
[PostgreSQL 15 permission change](https://www.postgresql.org/docs/15/release-15.html).

The inbox is permanent deduplication state. Quarantine and discard audit retention belong to the
application owner. Do not remove pending quarantine copies before recovery; deleting old inbox rows
can repeat business effects after a replay or restore.

## Bounded processing

All these environment settings are positive integers; zero cannot disable a deadline.

| Setting | Default | Purpose |
| --- | ---: | --- |
| `KND_CONSUMER_CONNECT_SECONDS` | 5 | JDBC connection and login deadline |
| `KND_CONSUMER_DB_POOL_SIZE` | 1 | Maximum reusable JDBC connections per process, 1–32 |
| `KND_CONSUMER_SOCKET_SECONDS` | 15 | JDBC socket read deadline |
| `KND_CONSUMER_STATEMENT_MS` | 10000 | PostgreSQL statement timeout within each transaction |
| `KND_CONSUMER_LOCK_MS` | 2000 | PostgreSQL lock wait timeout, at most the statement timeout |
| `KND_CONSUMER_DEADLINE_MS` | 20000 | Total database acquisition, handler and transaction deadline |
| `KND_CONSUMER_CLEANUP_MS` | 1000 | Additional allowance for cancellation and rollback |
| `KND_CONSUMER_PROGRESS_MS` | 5000 | Progress ACK interval during processing and recovery writes |
| `KND_CONSUMER_ACK_TIMEOUT_MS` | 5000 | Confirmed ACK and JetStream request deadline |
| `KND_CONSUMER_ACK_WAIT_MS` | 30000 | AckWait when **creating** the durable |
| `KND_CONSUMER_MAX_ACK_PENDING` | 1000 | Shared pending limit when **creating** the durable |

The application settings override timeout properties in the JDBC URL. TCP keepalive is enabled.
The example uses a bounded HikariCP pool to reuse authenticated connections. The connection limit also
bounds pool acquisition; increasing the pool does not create additional consumer workers. Run worker
replicas for concurrency. Timed-out connections are cancelled before their pool lease can be reused.
The processing deadline includes acquiring a connection, SQL, the handler, commit and connection
cleanup; parsing a size-bounded envelope happens before that transaction. Recovery writes have their
own bounded transaction. Tune the limits from measured handler latency and database/network behavior.

Progress must be less than half the live consumer's shortest acknowledgement wait. Backoff entries
override AckWait and are checked at startup. Provisioning is create-only; changing an environment
variable on a worker does not update an existing durable. Adjust its policy explicitly and restart
workers after validating the new interval. Progress ACKs do not extend the configured processing
deadline.

A timeout cancels processing and aborts the active connection. A handler returning after cancellation
cannot start a commit. A commit already in flight can have an ambiguous outcome; no ACK is sent and
the inbox resolves redelivery. If driver or application code ignores cancellation beyond the cleanup
allowance, the worker stops instead of accumulating abandoned tasks. Configure a supervisor restart
policy. Java cannot safely force-stop arbitrary application code; external effects remain outside the
transaction contract.

## Owner monitoring

`KND_CONSUMER_MONITOR_SECONDS` defaults to 30. Each report includes worker readiness, active work,
oldest processing age, completed work, retries, quarantines, drops and ACK failures. Logs contain no
payload, parser error text, database URL or remote exception message.

Set `KND_CONSUMER_HEALTH_PORT` to enable HTTP; the default `0` disables it. The default bind address is
`127.0.0.1`; override `KND_CONSUMER_HEALTH_BIND` for a protected monitoring network. Compose publishes
port 8081 on host loopback. `GET /health/ready` reports whether the worker is running, connected to
NATS and within its processing bound. It is not proof that application effects are caught up, or a
Keycloak readiness check. `GET /metrics` exposes:

- `knd_consumer_ready`, `knd_consumer_active`, `knd_consumer_oldest_processing_seconds`;
- `knd_consumer_last_commit_timestamp_seconds` (zero until the first commit/verified duplicate);
- `knd_consumer_received_total`, `committed_total`, `duplicates_total`, `retries_total`,
  `quarantined_total`, `dropped_total`, `timeouts_total`, `ack_failures_total`, `progress_failures_total`
  with the `knd_consumer_` prefix on every counter.

Counters describe attempts, so redelivery after a lost ACK can increment them again. Use database
audit identities for unique quarantine/discard totals. There are no per-event or per-realm metric
labels. Counters saturate at `Long.MAX_VALUE` rather than wrap negative, and active attempts use
object identities instead of a growing sequence. The HTTP endpoint has no authentication: bind and restrict it as an internal monitoring
endpoint. Alert on pending-window saturation, stalled commit rate while pending work exists, repeated
failures, quarantine growth, unexpected drops and database/broker storage pressure. Thresholds,
capacity planning and the end-to-end delivery SLO belong to the Keycloak/application owner.

## Retry, quarantine and explicit loss

All shedding is disabled by default. These variables belong to the example receiver, not the
provider; they do not delete Keycloak outbox rows or configure an independent NATS application.

| Variable | Default | Options / effect |
|---|---|---|
| `KND_CONSUMER_FAILURE_ACTION` | `retry` | `retry`, `quarantine` (save original before ACK), or `drop` (save discard audit before ACK) |
| `KND_CONSUMER_FAILURE_SUBJECTS` | unset | Comma-separated NATS patterns; mandatory for `quarantine` and `drop` |
| `KND_CONSUMER_FAILURE_MIN_DELIVERIES` | `5` | Rejection recovery threshold, 1–1,000,000 deliveries |
| `KND_CONSUMER_DROP_AFTER_SECONDS` | `0` | Optional age shedding, 1–31,536,000 seconds; requires `drop` and an explicit scope |

For example, to discard scoped login events once their broker publication is over an hour old:

```text
KND_CONSUMER_FAILURE_ACTION=drop
KND_CONSUMER_FAILURE_SUBJECTS=keycloak.events.*.user.login
KND_CONSUMER_DROP_AFTER_SECONDS=3600
```

To avoid capturing an unwanted class of events at all, use the provider's `KND_FILTER_FILE` instead.
Capture filtering changes future capture; it does not purge already accepted messages.

The default `KND_CONSUMER_FAILURE_ACTION=retry` retains every failed delivery. A poison event can fill
the pending window; an owner must repair processing or explicitly select a recovery policy. Unlimited
broker MaxDeliver remains required even when application recovery is enabled.

To quarantine rejected login events after five deliveries:

```text
KND_CONSUMER_FAILURE_ACTION=quarantine
KND_CONSUMER_FAILURE_SUBJECTS=keycloak.events.*.user.login
KND_CONSUMER_FAILURE_MIN_DELIVERIES=5
```

`FAILURE_SUBJECTS` is a required comma-separated list of NATS subject patterns for both `quarantine`
and `drop`. `*` matches one token; terminal `>` matches one or more. Delivery counts include lost ACKs,
process crashes and earlier retries, not just application errors. Invalid JSON, unsupported envelopes,
invalid event IDs and oversized payloads are deterministic rejections. Application handlers can throw
`RejectedEventException(HANDLER_REJECTED)` when they deliberately reject an event. Unexpected
exceptions, SQL failures, timeouts and event-ID/content conflicts remain retryable regardless of the
configured rejection policy.

Quarantine commits original bytes, headers, subject, identity and a fixed reason code before ACK.
`drop` commits only audit metadata and the payload hash before ACK; the payload is discarded and
cannot be replayed from that audit. A failed recovery write leaves the broker copy pending.
If a discard audit committed but its ACK was lost, changing the policy to quarantine does not turn
that audit into a recovery copy. The worker retains the redelivery and reports retries. Resolve that
identity deliberately: restore a verified quarantine copy with the migration/recovery identity, or
explicitly reinstate the discard policy. It never claims a payload-free audit is a successful quarantine.

For deliberate age-based load shedding, set `KND_CONSUMER_FAILURE_ACTION=drop`, an explicit subject
scope, and `KND_CONSUMER_DROP_AFTER_SECONDS` to a positive value. It defaults to **0 (disabled)**.
Messages in scope older than that threshold are audited and dropped before running a business
handler. Age uses the broker publication timestamp, not an event-supplied timestamp. This policy can
discard valid work and is independent of the rejection delivery threshold. Other subjects continue
through normal processing. Capture filters remain the cheapest way to avoid accepting unwanted work;
consumer shedding does not trim the Keycloak outbox.

## Inspect and replay

Run recovery with the same logical consumer name and database, using a separate operator identity
where appropriate:

```sh
java -jar consumer-example/target/consumer-example-1.0.0-SNAPSHOT.jar quarantine-list 100
java -jar consumer-example/target/consumer-example-1.0.0-SNAPSHOT.jar quarantine-replay KEYCLOAK_EVENTS 123
```

Listing returns metadata only. Repair the handler/schema first; replay preserves the original payload
and Nats-Msg-Id. Invalid envelopes stay invalid until a compatible handler is deployed. Replaying with
the same ID during JetStream's deduplication window may return a duplicate ACK even though WorkQueue
retention has already removed the original. The command leaves that quarantine record pending and
reports that the operator should retry after the window. It marks recovery only after a fresh publish
ACK from the original stream. A crash before that mark can repeat publication; the permanent inbox
still prevents duplicate database effects.

Replay republishes through the original stream and subject, so existing consumer filters still apply.
It does not purge, edit or create streams. Audit identities include consumer, stream and sequence;
retain those namespaces across normal restarts, and plan a namespace migration before deleting and
recreating a stream whose sequence numbers could collide with recovery records.
