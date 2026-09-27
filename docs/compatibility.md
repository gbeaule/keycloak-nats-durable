# Keycloak compatibility and extension boundaries

The provider uses Keycloak's event listener, provider discovery, session/transaction lifecycle and shared JPA entity manager. It requires **the existing Keycloak PostgreSQL database**, not an additional database instance, JDBC URL, connection pool or database password. Hibernate ORM maps `OutboxEvent` to `KC_NATS_OUTBOX`; the relay query is JPQL. Liquibase adds the extension's table/index using Keycloak's configured database/schema. It does not edit Keycloak's own tables or install triggers.

## Accepted extension API decision

The product uses Keycloak's documented event-listener and custom JPA registration APIs. This decision is accepted: keeping capture in Keycloak's existing database transaction is the required durability mechanism. Compatibility is maintained through explicit version tests and small, localized integrations with Keycloak and Hibernate, rather than speculative adapters for untested releases.

Keycloak explicitly labels [`JpaEntityProvider` / `JpaEntityProviderFactory`](https://www.keycloak.org/docs/latest/server_development/index.html#_extensions_jpa) unsupported: these documented hooks can change or disappear without compatibility guarantees. Using the official mechanism does **not** turn it into an API with a stability promise. The implementation also imports lifecycle/transaction utilities from Keycloak server modules; a successful build alone is insufficient evidence for a new server version. This project cannot promise vendor support on Keycloak's behalf.

The documented [event listener contract](https://www.keycloak.org/docs-api/26.7.4/javadocs/org/keycloak/events/EventListenerProvider.html) describes transactional JPA work. However, [`EventListenerSpi.isInternal()`](https://github.com/keycloak/keycloak/blob/26.7.4/server-spi-private/src/main/java/org/keycloak/events/EventListenerSpi.java) also returns true. The tested server logs `KC-SERVICES0047` for both `eventsListener` and `jpa-entity-provider`. This is not a warning caused by reflection or patching Keycloak: even the documented event-listener mechanism lacks a stability guarantee. “Documented extension API,” “public Java interface,” and “vendor-supported stable API” are different claims.

Adding the outbox entity to that same entity manager makes the event and account change one database commit. An after-commit NATS callback alone leaves a crash gap, and publishing before commit can expose rolled-back account changes.

The [review of cevheri/keycloak-custom-event-listener](listener-comparison.md) found direct synchronous webhook delivery without durable retries or commit coordination. Its smaller API footprint does not preserve the required guarantees. The outbox's migration remains the schema authority; a separate `initdb` script is not required.

## Version policy

The full compatibility matrix runs after changes reach `main` and on manual requests. PRs run
quick checks; use `python3 scripts/validate.py matrix` for full local validation before merging.
See [local validation and CI](ci.md) for focused runs and the required-check policy.

CI runs the same provider code and dependency baseline against Keycloak **26.6.4** and **26.7.4**, on PostgreSQL 18.6 and Java 21. The Maven compile baseline is `keycloak.version` (currently 26.7.4); the test server image is controlled separately by `keycloak.runtime.version`. Exact local results are recorded in [testing](testing.md). Entries in CI are compatibility targets until the corresponding run passes; untested versions, other database engines, vendor distributions and mixed-version Keycloak rolling upgrades are not implied.

PostgreSQL 18 is not an installation requirement. The extension uses the database already configured
for Keycloak and performs no PostgreSQL-major-version gate. Target PostgreSQL **14, 15, 16, 17 and 18**
where the selected Keycloak release and PostgreSQL vendor support that major; use its maintained minor
release. The current [Keycloak database matrix](https://www.keycloak.org/server/db) lists 14–18.
Older unsupported majors and other database engines are outside this compatibility claim.

The full runtime matrix uses 18.6. A separate CI job exercises 14–17 with the current Keycloak baseline:
schema installation, transactional rollback and synchronous commit, retry/vacuum settings, consumer
deadlines/recovery, and persisted-outbox upgrade. These targeted checks do not imply that every
database/Keycloak combination has run the full cluster and physical-recovery suite. The selected
image is configurable for local verification, including a deployment's exact minor or digest:

```sh
mvn -B -ntp -Pintegration -Dpostgres.image=postgres:17-alpine verify
```

The versioned Compose image is a demonstration default. Keep an existing supported production
database; a major upgrade is optional and uses a separate [migration procedure](postgres-upgrade.md).

```sh
mvn -B -ntp -Pintegration -Dkeycloak.runtime.version=26.6.4 verify
mvn -B -ntp -Pintegration -Dkeycloak.runtime.version=26.7.4 verify
```

Before adding a version, run compilation, unit tests, schema installation, rollback, broker/Keycloak restart and clustered relay tests. Keep explicit matrix entries rather than assuming a Maven version range proves compatibility. Keycloak's own supported upgrade procedure still applies; test against a restored copy of the deployment database before upgrading an existing installation.

## Adding a Keycloak release

The runtime matrix lives in [`config/keycloak-versions.json`](../config/keycloak-versions.json). The `verify` GitHub Actions workflow also accepts an optional `keycloak_version` through **Run workflow**. It adds that candidate to the regular matrix and runs the complete suite with the existing compile baseline. It does not silently declare the candidate supported or change the checked-in matrix. Input is restricted to explicit `major.minor.patch` releases and passed to Maven through a quoted environment variable.

For a local candidate run, replace `X.Y.Z` with the actual release:

```sh
mvn -B -ntp -Pintegration -Dkeycloak.runtime.version=X.Y.Z verify
```

The suite includes an offline replacement test: boot `keycloak.upgrade.from` (26.6.4 by default), stop NATS, commit account events, stop that Keycloak process, then boot the candidate against the **same database**. Pending IDs, subjects and payloads must survive schema startup unchanged and drain when NATS returns. New events must still be captured. A target equal to the source tests same-version reinstallation. This does not claim support for mixed-version rolling upgrades or downgrades. The source version can be set with `-Dkeycloak.upgrade.from=...` to exercise a deployment's supported upgrade path.

When the candidate passes, review upstream changes to the listener transaction contract, custom entity registration/migration, asynchronous commit handling, Hibernate row-lock hints and event enums. If compilation against a newer API is needed, change `keycloak.version` and rerun the **whole existing runtime matrix**, including the upgrade test. `EventSchemaTest` deliberately compares compiled enums with the checked-in catalogue; added or removed types require reviewing the catalogue, schema, examples and subject contract, not disabling that assertion. Keep the catalogue version in its filename and test reference aligned with the documented compile baseline. Add the new runtime to the matrix only with passing evidence recorded in [testing](testing.md).

Keycloak-specific lifecycle and transaction work is concentrated in `DurableEventListener` and its factory; custom entity discovery is in `OutboxEntityProviderFactory`; the Hibernate query and symbolic timeout are in `OutboxRepository`. These are the first review points when an upstream API changes. A runtime test validates the packaged provider inside the server, including linkage and migration; a successful Maven compilation alone is insufficient.

## Existing installations and clusters

Install the bundled provider JAR on every node, rebuild the Keycloak image/distribution, allow the schema migration at startup, configure NATS, then enable `nats-durable` in each required realm. An existing supported PostgreSQL deployment does not need to replace or export/import its database. This is a provider installation and restart, not hot deployment. Database DDL permissions (or an operator-managed migration process) are required for the new table and Keycloak's extension migration bookkeeping.

Use the same stream, NATS account, subject prefix and provider version on all nodes sharing an outbox. An outbox row stores its subject and payload, not its destination NATS account: inconsistent node configuration could route events into different systems. Do not repoint a deployment while it has a pending backlog. A relay is started on each node only after migration; each publication holds a row lock until the database transaction finishes. Nodes skip rows claimed elsewhere, and a failed node's rows become eligible once PostgreSQL releases its locks. Clustering needs no leader election in this provider. PostgreSQL HA, Keycloak clustering configuration and JetStream replication remain deployment responsibilities.

No database or NATS work is performed in the local wakeup callback. A wakeup is only a latency hint; another node or a restart recovers committed rows through periodic scans. Losing a wakeup cannot lose an event.
