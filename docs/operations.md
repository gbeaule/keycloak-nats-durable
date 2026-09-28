# Operations

## Installation

The provider uses the PostgreSQL database already configured for Keycloak. Installing it does not
require another database or pool. Use the [compatibility process](development.md#compatibility) for
the intended Keycloak and database versions; demo image versions are not installation requirements.

1. Back up the Keycloak database. Install the built provider JAR in `$KEYCLOAK_HOME/providers/` on
   every node, then run `kc.sh build --db=postgres`.
2. Provision the JetStream stream and durable consumers with an administrative identity. The
   example's [provision command](../consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/ConsumerMain.java)
   creates a WorkQueue setup. Independent applications needing the same events require Limits
   retention and separate durables. Provisioning does not migrate existing resource policies.
3. Configure the provider's NATS destination and credentials, then restart Keycloak. Its
   [Liquibase migration](../extension/src/main/resources/META-INF/nats-outbox-changelog.xml) owns the
   outbox schema; allow the required DDL through your deployment's migration process.
4. Add `nats-durable` to each required realm's event listeners, preserving other listeners.
   [The demo realm](../deploy/realm.json) shows the configuration. Saving Keycloak events and including
   admin representations are not prerequisites.
5. Exercise an account operation and verify its downstream effect. Monitor delivery separately from
   Keycloak readiness.

All nodes sharing an outbox must use the same provider version, NATS account, stream and subject
prefix. Configure Keycloak clustering independently. Drain pending work before removing the provider;
changing destinations or routing while a backlog exists requires an explicit migration plan.

## Configuration sources

Exact names, defaults and validation rules belong to these files:

| Concern | Source |
| --- | --- |
| Provider settings and Keycloak SPI precedence | [BridgeConfig](../extension/src/main/java/io/github/gbeaule/keycloaknats/BridgeConfig.java) |
| Capture policy and reload behavior | [EventFilter](../extension/src/main/java/io/github/gbeaule/keycloaknats/EventFilter.java), [CaptureScope](../extension/src/main/java/io/github/gbeaule/keycloaknats/CaptureScope.java) and [ReloadingEventFilter](../extension/src/main/java/io/github/gbeaule/keycloaknats/ReloadingEventFilter.java) |
| TLS | [TlsConfig](../nats-transport/src/main/java/io/github/gbeaule/keycloaknats/tls/TlsConfig.java) |
| Accepted stream configuration | [StreamPolicy](../nats-transport/src/main/java/io/github/gbeaule/keycloaknats/jetstream/StreamPolicy.java) |
| Example consumer, provisioner and collector | [ConsumerConfig](../consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/ConsumerConfig.java) and [command entry point](../consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/ConsumerMain.java) |
| Local deployment wiring | [Compose](../compose.yaml) and [deployment files](../deploy) |

Environment names use the `KND_` prefix. Matching Keycloak provider keys, such as `nats-url`,
take precedence over environment values.

For an encrypted NATS connection, configure `KND_NATS_URL` with reachable `tls://` seeds and mount
credentials through `KND_CREDENTIALS_FILE`. Use `KND_TLS_CA_FILE` for a private CA; mutual TLS also
uses the paired `KND_TLS_CERT_FILE` and `KND_TLS_KEY_FILE`. Certificates must cover seed and discovered
server names. Plaintext connections use `nats://`; mixed modes are rejected. Rotate certificate and
credential bundles, then roll clients to load them and verify delivery before revoking old identities.

Leave `KND_FILTER_FILE` unset to capture all events reaching the listener, or mount a deployment-owned
policy directory and point it to the chosen file. Start from the optional [all-events](../config/events-all.json),
[disabled-user](../config/events-disabled-only.json) or [scoped](../config/events-scoped.json) example.
Replace placeholder realm IDs; these files are not packaged or loaded automatically.

Replace policy files atomically within the mounted directory. Invalid initial policies fail startup;
invalid reloads retain the last valid policy. Deleting a configured file does not restore capture-all.
Policy reloads affect future capture and are eventually consistent across nodes. Other settings
require a restart.

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

Monitor oldest pending age and queue growth across the outbox, stream and consumers. Include database
and broker disk headroom, replication health, pool pressure, redeliveries, quarantine and stalled
processing. An empty outbox proves publication, not completion of downstream effects. Use a canary
account operation to measure end-to-end delivery.

The packaged [OutboxReport](../consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/OutboxReport.java)
collector emits JSON or Prometheus output using a read-only database identity. It reads backlog
metadata without payloads. Its settings live in `ConsumerConfig`; run its main class with the consumer
JAR on the classpath and `json` or `prometheus` as the argument. Combine it with the example
[consumer metrics](../consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/ConsumerMonitor.java)
and adapt the [alert rules](../deploy/prometheus-rules.yml) to your delivery objective. Alert on failed
or stale collection as well as backlog thresholds.

Budget outage storage from accepted event rate × outage duration × measured storage per event,
including indexes, WAL, replication and vacuum headroom. Recovery requires sustained delivery and
processing rates above ongoing capture. More relay workers consume more database connections during
broker requests; measure Keycloak request latency and pool pressure before increasing concurrency.
Use the [benchmark](../integration-tests/src/test/java/io/github/gbeaule/keycloaknats/ThroughputBenchmark.java)
on representative infrastructure rather than adopting workstation measurements.

Local discard diagnostics expire automatically, independently of NATS. Maintenance settings in
[AuditCleanupConfig](../extension/src/main/java/io/github/gbeaule/keycloaknats/AuditCleanupConfig.java)
apply to existing history after restart; they do not change captured publication policies.
Cleanup touches only audit history. Protected pending events and per-user counters still require
capacity planning.

[AuditRepository](../extension/src/main/java/io/github/gbeaule/keycloaknats/AuditRepository.java)
provides bounded, read-only metadata pages and aggregate inspection on a managed session. Start
inspection with `(Long.MIN_VALUE, "")`, then use the last result's discard time and ID as the cursor.
Keycloak's metrics endpoint exposes the aggregate signals registered by
[AuditCleanupMetrics](../extension/src/main/java/io/github/gbeaule/keycloaknats/AuditCleanupMetrics.java).
Row counts and eligible age are database-wide observations cached after successful sweeps; deletion
and failure counters are per node. Alert on stale successful-sweep timestamps as well as growing
eligible age. Runtime cleanup needs SELECT, UPDATE (for row locks) and DELETE on the audit table,
plus schema USAGE; it needs no DDL privileges.

Confirmed publication deletes outbox rows; ordinary vacuum makes their space reusable. Protected pending
events have no expiry. Expand capacity, narrow future capture deliberately, or stop admitting relevant writes
before the shared database fills. Purging pending events or aging out inbox identities sacrifices
delivery guarantees.

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
