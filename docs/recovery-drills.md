# Production acceptance and recovery drills

Run the isolated reference drills with Java 21, Maven and Docker:

```sh
mvn -B -ntp -Pintegration \
  -Dit.test=ProductionModeIT,PostgresRecoveryIT \
  -Dfailsafe.failIfNoSpecifiedTests=false verify
```

The tests create disposable containers and generated credentials. They do not connect to an existing
deployment. Keep their JUnit reports and container logs with the release evidence. Repeat equivalent
procedures in a staging copy of the actual topology before approving deployment.

## Optimized production startup and credentials

`ProductionModeIT` builds `deploy/Dockerfile.keycloak`, starts it with `start --optimized`, verifies
HTTPS with a generated trusted CA and hostname checks, and checks `/health/ready` on the internal
management port. It publishes through four relay workers using a scoped JWT credentials file. The
publisher can publish events, read the intended stream's configuration and receive reply inboxes;
an attempted stream deletion is denied. Broker unavailability leaves Keycloak ready and retains
committed outbox rows. Revoking the old JWT user, reconnecting clients and rolling Keycloak with a new
credentials file drains those same rows. The test checks that credential contents are absent from
provider logs.

The test's disposable MEMORY account resolver loads updated claims on a broker restart. A deployed
operator/account resolver needs its own tested account-JWT update and revocation propagation steps.
For routine rotation, introduce the replacement identity, mount its credentials atomically, roll
clients, verify publishing and then revoke the old identity. A compromise may require immediate
revocation and forced disconnects; the outbox retains work during that interruption.

Use `KND_CREDENTIALS_FILE` with scoped NATS user JWTs for independent publisher and consumer identities.
A server-wide `KND_TOKEN` authenticates access to the server; it does **not** establish per-user subject
permissions, even when `default_permissions` appears in the server configuration. Provision stream
and consumer resources with a separate administrative identity. NATS TLS/mTLS, quorum loss and leader
failure are exercised separately by `NatsClusterIT`.

The reference HTTPS endpoint is direct to Keycloak. Validate the actual ingress/proxy headers,
hostname, certificate renewal, network policy, probes and supervisor restart behavior separately.
Keep delivery health separate from Keycloak readiness. Run migrations with a migration identity, then
exercise capture, relay and consumer processing with the intended restricted database roles; see
[database ownership](database.md) and [consumer permissions](consumer.md).

## Coordinated cold backup and restore

`PostgresRecoveryIT.coordinatedColdBackupRestoresPendingCaptureAndPreviouslyCommittedInbox` processes
and ACKs one event, stops the broker, commits another account change and quiesces Keycloak. It takes a
custom-format PostgreSQL dump and a cold copy of JetStream storage at that common boundary, replaces
both containers, restores the data, and restarts the provider. It verifies the account row and exact
pending payload survived, the previously committed inbox suppresses duplicate effects, the pending
event gets one effect, and the WorkQueue empties after ACK.

The reference places Keycloak and consumer tables in one PostgreSQL database so the dump is a single
consistent database snapshot. When those databases are separate, quiesce producers, relays and
consumers, establish and record a common recovery boundary, then back up **all** stores and downstream
effects. A running filesystem copy of JetStream is not the cold-copy procedure in this drill. For
online backups, use the broker's supported snapshot procedure and validate the chosen recovery point.

For the deployment runbook:

1. Record the source release, schema versions, stream/durable configuration and recovery timestamp.
   Fence writers and consumers, wait for active transactions, and record outbox, broker and inbox state.
2. Take and verify the database backups/WAL and broker snapshots at the coordinated boundary. Preserve
   identities, payload bytes, stream configuration, consumer state and deduplication history.
3. Restore into isolated fresh storage. Preserve the original storage and backups for rollback.
4. Check account/outbox consistency and retained broker messages; start consumers against their
   matching inbox/effect database. Verify duplicate IDs do not repeat transactional effects.
5. Start relays and a canary account operation. Account for every pending ID and compare restored data
   with the agreed recovery point before reopening traffic. Record actual recovery duration and loss.

Restoring a consumer database alone can erase an effect and its inbox entry after a WorkQueue ACK has
removed the only broker copy. Restoring a stale outbox against newer broker deduplication state can
also suppress a needed republish. Neither problem is repaired by restarting a worker. Coordinate the
stores or supply an independently retained, identity-preserving recovery log. Recovery to an older
point has an explicit RPO; this bridge does not reconstruct writes beyond that point.

## Synchronous PostgreSQL promotion

`PostgresRecoveryIT.fencedSynchronousPromotionPreservesAcknowledgedAccountAndPendingEvent` takes a
physical base backup, starts a streaming standby and waits until PostgreSQL reports it as synchronous.
With `fsync=on`, `synchronous_commit=on` and `synchronous_standby_names` configured, it commits an account
change while the broker is down. It hard-stops the primary, promotes the standby, reconnects Keycloak,
restores broker access and verifies the original account, event bytes and single consumer effect.

The hard stop is the reference fence: the old primary is never restarted as a writer. Use the actual
HA manager, fencing mechanism and endpoint change in staging; never promote a second writable primary
while the old one can still accept writes. An asynchronous replica may lack acknowledged commits.
Measure replica lag, failure detection and recovery time under load, and test connection-pool recovery.
The drill removes the old synchronous-standby requirement after promotion because it has only one
remaining database. Production must restore the required redundancy before claiming its normal HA
guarantee. Do not reuse the test's superuser credentials or permissive replication network rules.
