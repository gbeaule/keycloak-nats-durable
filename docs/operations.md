# Operations

## Installation

The provider uses the PostgreSQL database already configured for Keycloak. Installing it does not
require another database or pool. Use the [compatibility process](development.md#compatibility) for
the intended Keycloak and database versions; demo image versions are not installation requirements.

1. Back up the Keycloak database. Install the built provider JAR in `$KEYCLOAK_HOME/providers/` on
   every node, then run `kc.sh build --db=postgres`.
2. Provision a safe JetStream stream with an administrative identity. The standalone
   [stream fixture](../deploy/stream.json) uses WorkQueue retention and requires no consumer.
   Adapt replication and capacity to the deployment. Receiving applications provision their own
   subscriptions; independent applications may require Limits retention and separate durables.
3. Configure the provider's NATS destination and credentials, then restart Keycloak. Its
   [Liquibase migration](../extension/src/main/resources/META-INF/nats-outbox-changelog.xml) owns the
   outbox schema; allow the required DDL through your deployment's migration process.
4. Add `nats-durable` to each required realm's event listeners, preserving other listeners.
   [The demo realm](../deploy/realm.json) shows the configuration. Saving Keycloak events and including
   admin representations are not prerequisites.
5. Exercise an account operation and verify NATS acceptance and source resolution. Monitor publication
   separately from Keycloak readiness; downstream processing is outside extension health.

All nodes sharing an outbox must use the same provider version, NATS account, stream and subject
prefix. Configure Keycloak clustering independently. Drain pending work before removing the provider;
changing destinations or routing while a backlog exists requires an explicit migration plan.

For a standalone smoke test, follow the [local startup commands](../README.md#build-and-try-locally).
Disable a demo user, confirm the stream count increases and the outbox drains, and verify the stream
has no consumers. Stop NATS, repeat an account operation, then restart Keycloak: protected work must
remain pending. Restart NATS and confirm publication resumes. Keycloak's local metrics are available
on port 9000. The automated [publisher scenario](../integration-tests/src/test/java/io/github/gbeaule/keycloaknats/PerUserPublicationIT.java)
also exercises discard, cleanup failure and recovery during an outage.

## Configuration sources

Exact names, defaults and validation rules belong to these files:

| Concern | Source |
| --- | --- |
| Provider settings and Keycloak SPI precedence | [BridgeConfig](../extension/src/main/java/io/github/gbeaule/keycloaknats/BridgeConfig.java) |
| Capture policy and reload behavior | [EventFilter](../extension/src/main/java/io/github/gbeaule/keycloaknats/EventFilter.java), [CaptureScope](../extension/src/main/java/io/github/gbeaule/keycloaknats/CaptureScope.java) and [ReloadingEventFilter](../extension/src/main/java/io/github/gbeaule/keycloaknats/ReloadingEventFilter.java) |
| TLS | [TlsConfig](../nats-transport/src/main/java/io/github/gbeaule/keycloaknats/tls/TlsConfig.java) |
| Accepted stream configuration | [StreamPolicy](../nats-transport/src/main/java/io/github/gbeaule/keycloaknats/jetstream/StreamPolicy.java) |
| Standalone collector settings | [ConsumerConfig.report](../consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/ConsumerConfig.java) |
| Local deployment wiring | [Producer Compose](../compose.producer.yaml) and [deployment files](../deploy) |

Environment names use the `KND_` prefix. Matching Keycloak provider keys, such as `nats-url`,
take precedence over environment values.

For an encrypted NATS connection, configure `KND_NATS_URL` with reachable `tls://` seeds and mount
credentials through `KND_CREDENTIALS_FILE`. Use `KND_TLS_CA_FILE` for a private CA; mutual TLS also
uses the paired `KND_TLS_CERT_FILE` and `KND_TLS_KEY_FILE`. Certificates must cover seed and discovered
server names. Plaintext connections use `nats://`; mixed modes are rejected. Rotate certificate and
credential bundles, then roll clients to load them and verify delivery before revoking old identities.

Leave `KND_FILTER_FILE` unset to capture all events reaching the listener, or mount a deployment-owned
policy directory and point it to the chosen file. Start from the optional [all-events](../config/events-all.json),
[disabled-user](../config/events-disabled-only.json), [scoped](../config/events-scoped.json) or
[publication-policy](../config/events-with-delivery.json) example. The latter explicitly protects
user lifecycle events and makes only short-lived login events discardable.
Replace placeholder realm IDs; these files are not packaged or loaded automatically.

Replace policy files atomically within the mounted directory. Invalid initial policies fail startup;
invalid reloads retain the last valid policy. Deleting a configured file does not restore capture-all.
Policy reloads affect future capture and are eventually consistent across nodes. Other settings
require a restart.

The relay worker count bounds producer concurrency. Batch size and polling intervals bound normal
and expiry scans; expiry remains eligible during retry backoff. Audit retention, sweep interval,
batch limits and transaction timeout are independent maintenance settings. These use the same
provider-over-environment precedence. Event publication policy belongs only in the filter file;
receiver deadlines and application processing settings do not belong there.

## Storage and access

Use durable PostgreSQL commits and replicated JetStream File storage across independent failure
domains. Keep PostgreSQL `fsync` and durable `synchronous_commit` settings, and align replication with
the required failover guarantee. Configure NATS disk synchronization for the required power-loss
guarantee. Test the actual storage and HA topology; replica counts alone cannot establish durability.

Separate provisioning, publisher, consumer and monitoring identities. The publisher needs event
publication, stream-info access and its reply inbox, without stream administration rights.
Consumers need their durable's information, pull and ACK permissions. A shared server token does not
establish per-user authorization. Restrict monitoring endpoints and protect event identifiers and
recovery copies as application data.

The example consumer's schema belongs to
[consumer.sql](../consumer-example/src/main/resources/db/consumer.sql). Run its `migrate` command with
a migration identity before starting restricted workers; automatic migration is opt-in for the demo.
Preserve inbox state across restarts.

## Monitoring and capacity

Monitor original backlog and oldest capture age, blocked users, retry activity, local discards,
outcome uncertainty and cleanup health. Include database and broker disk headroom, replication and
pool pressure. An empty outbox means publication obligations have resolved through acceptance or
authorized discard; it proves neither application receipt nor processing. Shared database or broker
failures can affect all users.

The packaged [OutboxReport](../consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/OutboxReport.java)
collector emits JSON or Prometheus output using a read-only database identity with these
[column grants](../deploy/outbox-monitor.sql). Run its main class from the example JAR with `json`,
`prometheus` or `audit` as the argument. Only its report settings are loaded: no consumer program,
consumer database or NATS connection is required. `audit` reads a bounded metadata page; pass
`audit <afterDiscardedAt> <afterId> <limit>` to continue after its last row.

The report counts users with selected events behind an unresolved predecessor. Predecessor checks
include rows outside the report's subject filter. Retry-head counts include userless work, and
unknown-publication counts conservatively include committed send intent. Audit counts describe
retained history, not lifetime totals. Set the collector's audit retention to match the provider.

Keycloak exposes node activity through [RelayMetrics](../extension/src/main/java/io/github/gbeaule/keycloaknats/RelayMetrics.java)
and [AuditCleanupMetrics](../extension/src/main/java/io/github/gbeaule/keycloaknats/AuditCleanupMetrics.java).
Publication failures count failed calls, even if their database transaction later rolls back;
publication, retry and discard resolutions count only confirmed source commits. Counters reset with
the process. Discard reasons are bounded labels; event/user IDs appear only in database diagnostics.
Adapt the [alert rules](../deploy/prometheus-rules.yml) to your objective and collection interval.
Alert on failed or stale collection as well as backlog thresholds. Do not sum database-wide gauges
across replicas or treat consumer cursors and effects as extension health.

Budget outage storage from accepted event rate × outage duration × measured storage per event,
including indexes, WAL, replication and vacuum headroom. Recovery requires sustained publication
above ongoing capture. More relay workers consume more database connections during
broker requests; measure Keycloak request latency and pool pressure before increasing concurrency.
Use the [benchmark](../integration-tests/src/test/java/io/github/gbeaule/keycloaknats/ThroughputBenchmark.java)
on representative infrastructure rather than adopting workstation measurements.

Local discard diagnostics expire automatically, independently of NATS. Maintenance settings in
[AuditCleanupConfig](../extension/src/main/java/io/github/gbeaule/keycloaknats/AuditCleanupConfig.java)
apply to existing history after restart; they do not change captured publication policies.
Cleanup defaults to seven days; zero retention makes audits eligible on the next cleanup pass.
It touches only audit history. Protected pending events and compact per-user counters still require
separate capacity planning; counters survive user deletion and outbox draining.

Cleanup row counts and eligible age are cached after successful sweeps; scrapes never query the
database. Runtime cleanup needs SELECT, UPDATE (for row locks) and DELETE on the audit table, plus
schema USAGE; it needs no DDL privileges. Metadata inspections explain the discarded event, rule,
reason, time and outcome uncertainty without storing its original payload. They are not a replay feed.

Confirmed publication deletes outbox rows; ordinary vacuum makes their space reusable. Protected pending
events have no expiry. Expand capacity, narrow future capture deliberately, or stop admitting relevant writes
before the shared database fills. Purging protected pending events sacrifices publication guarantees.

## Consumer recovery

Restore broker, network or credential availability to resume automatic outbox retries. Repair unsafe
stream settings without deleting backlog. If a stream fills, recover consumers or add capacity.
Preserve original IDs and payloads when investigating schema failures or conflicting duplicate IDs.

The example retries failed deliveries by default. Its
[failure policy](../consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/FailurePolicy.java)
allows explicitly scoped quarantine or discard, including opt-in age shedding. Database failures,
timeouts and identity conflicts remain retryable. A discard audit cannot recover a discarded payload.

Use the example's `quarantine-list` and `quarantine-replay` commands with the original consumer
namespace and database after fixing the processing issue. Replay preserves identity and may remain
pending until the broker deduplication window passes. Keep pending recovery copies and inbox history;
stream recreation or namespace changes need a recovery plan. Command syntax lives in
[ConsumerMain](../consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/ConsumerMain.java).

## Upgrades and coordinated restore

This ordering feature is unreleased and updates initial schemas directly. There is no old-data
migration or mixed-version rollout. Use fresh, separately named development storage for these
schemas; existing developer volumes are never deleted automatically.

Test provider and schema upgrades on a restored database with pending events. PostgreSQL major
upgrades require a rehearsed database migration into appropriate storage; changing an image tag does
not upgrade an existing data volume. Rehearse broker upgrades against existing stream and consumer
state as well. Fresh-container tests do not establish a production upgrade path.

Back up and recover Keycloak/outbox, JetStream, consumer inbox and business effects at a coordinated
boundary. Restoring only the consumer database can erase an effect after WorkQueue retention has
removed the broker copy. Restoring an older outbox can replay events or encounter newer broker
deduplication state. Restarting workers cannot repair inconsistent recovery points.

For a restore rehearsal, quiesce writers, relays and consumers; record pending identities and take
verified backups of every store. Restore into isolated storage, check account/outbox consistency and
consumer deduplication, then account for pending work and verify a canary before reopening traffic.
Preserve the original storage until acceptance completes.

The [production startup tests](../integration-tests/src/test/java/io/github/gbeaule/keycloaknats/ProductionModeIT.java)
and [database recovery tests](../integration-tests/src/test/java/io/github/gbeaule/keycloaknats/PostgresRecoveryIT.java)
provide executable reference scenarios. Repeat equivalent drills using the deployment's actual
ingress, credential rotation, backups and HA manager, including fencing the old database primary.
Record recovery time, data loss and restored redundancy.
