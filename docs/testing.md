# Verification record

Verified locally on 2026-09-25 using Windows 11, Docker Desktop / Docker Engine 29.8.0, Maven 3.9.6 and IntelliJ's Java 21.0.10 runtime. Tests use Keycloak 26.6.4 and 26.7.4, PostgreSQL 17.6 and NATS 2.12.8. Testcontainers 2.0.5 manages isolated containers and cleans them up.

| Suite | Passed | Failed | Skipped |
|---|---:|---:|---:|
| Extension unit and event-contract tests | 274 | 0 | 0 |
| Consumer configuration unit tests | 10 | 0 | 0 |
| Real-service integration tests on Keycloak 26.7.4 | 19 | 0 | 0 |
| Total | **303** | **0** | **0** |

The full 26.7.4 verification has **303 passing tests**, zero failures and zero skipped tests. Command: `mvn -B -ntp -Pintegration verify`, after applying `mvn spotless:apply`. Local runs set Java 21 in `JAVA_HOME` and used a workspace-local Maven cache. Spotless formatting and the full Google Checkstyle ruleset both pass, including mandatory braces. The existing-database upgrade case starts on 26.6.4 and replaces the server with 26.7.4 while events remain pending.

Both runtime targets use provider code compiled against the same Keycloak 26.7.4 API baseline; this is runtime compatibility testing, not just separate successful compilations. The table counts distinct tests, not repeated executions across runs. Candidate runs are available through the `verify` workflow's optional `keycloak_version` input; see [the compatibility process](compatibility.md).

The full compatibility command `mvn -B -ntp -Pintegration -Dkeycloak.runtime.version=26.6.4 verify` also passed all **303 tests**, including all 19 integration scenarios, with zero failures and skips. Its replacement scenario installs the same Keycloak version over the existing database. Both full runs took approximately five minutes locally.

A final naming-only cleanup changed logger, mapper and test-helper identifiers to Google Style. `mvn spotless:apply verify` then recompiled every module and passed all **284 unit tests**, formatting and Checkstyle checks. The two Docker runs above preceded those identifier renames; their behavior was unchanged. That final check is saved in `.work/final-style-unit-verify.log` and `.work/race-evidence/final-style/`.

Event coverage includes all 132 user event enums, all 39 admin resource enums paired with all four operation enums, null-omission/privacy rules, custom resource routing, twelve schema-validated JSON examples and a catalogue checked against the actual compiled enums. The admin resource combinations run inside parameterized cases and are not inflated into separate test counts.

Concurrency regressions use latches and controlled blocking to exercise commits between scan/wait, waking a sleeping worker, burst coalescing, zero-delay backlog draining, shutdown before worker startup and interruption. Publisher tests close during initial connection creation, stream validation and publication, verify disposal of a late connection, serialize simultaneous first publications, retry failed connection establishment, check idempotent close and preserve interruption. Relay tests stop or interrupt while acquiring the database row and retain the original event. A numeric-boundary test starts at `Long.MAX_VALUE - 1` and verifies saturated attempts continue scheduling retries without changing event identity.

## Integration scenarios

1. Login and failed-login events when Keycloak's own event store is disabled.
2. Account creation, disablement and deletion, with target identity and without admin representations.
3. An unacknowledged message is delivered to another worker; a confirmed acknowledged message stops redelivering.
4. An injected database constraint failure rejects the account update and rolls it back in both the database and the API's observed state.
5. NATS outage, five committed account operations, and a hard Keycloak restart while NATS remains unavailable; all five events are subsequently delivered.
6. A database trigger pauses the relay after JetStream accepts an event but before outbox deletion commits. Keycloak is killed; restart retries the original ID and JetStream stores one message.
7. A full DiscardNew stream leaves excess events in PostgreSQL until consumer ACK frees capacity.
8. Changing a stream to a lossy policy pauses publishing; repairing it drains the retained outbox.
9. Consumer database commit followed by simulated process death before ACK; redelivery does not repeat the business effect.
10. Business handler failure rolls back both the inbox entry and its database effect.
11. Sixteen concurrent attempts at the same event produce one committed effect.
12. Reusing an existing event ID with different content is rejected.
13. Two real Keycloak nodes drain concurrent account events; a committed, due row held locked cannot block delivery of the next due row, and it is delivered after release.
14. A hard NATS restart preserves pending messages and durable acknowledgement state.
15. Replay after JetStream's deduplication window expires still produces one consumer database effect.
16. Uncommitted and rolled-back outbox rows are never published.
17. A committed row with no local notification is recovered by a periodic scan.
18. A deferred PostgreSQL trigger checks that authentication-only outbox writes and relay removals do not commit with `synchronous_commit=off`.
19. With NATS stopped, capture account creation, disablement and deletion on 26.6.4, replace Keycloak against the same database, verify unchanged outbox IDs/subjects/payloads, restore NATS and verify delivery and new capture. The 26.7.4 target exercises an upgrade; the 26.6.4 target exercises same-version reinstallation.

See [DurabilityIT](../integration-tests/src/test/java/io/github/keycloaknats/DurabilityIT.java), [UpgradeIT](../integration-tests/src/test/java/io/github/keycloaknats/UpgradeIT.java) and the [test infrastructure](../integration-tests/src/test/java/io/github/keycloaknats/IntegrationSupport.java). Tests do not use `disabledWithoutDocker` or silently skip unavailable infrastructure. Read JUnit XML under `integration-tests/target/failsafe-reports` and logs under `integration-tests/target` for a local run's evidence. Upgrade source and target logs have separate filenames.

Maven replaces `target` reports on subsequent runs. Full reports, logs and provider artifact hashes for both runs are preserved under `.work/race-evidence/keycloak-26.7.4/` and `.work/race-evidence/keycloak-26.6.4/` (workspace-only, not committed). CI uploads a separate artifact for each runtime version. The workflow YAML was also parsed locally and its actual matrix-generation script checked with seven inputs, including valid, duplicate and invalid release values; extra Maven arguments and shell syntax were rejected.

## Packaged application checks

During the initial implementation, the Docker Compose images were built and run from the generated JARs. Re-running provisioning against the same stream/consumer succeeded. The executable consumer processed a real login and temporary account create/update/delete exactly once into `knd_effects`; the outbox reached zero. During a second login with NATS stopped, the outbox retained one row. After restart, the same running consumer recovered, the effect count increased by one, and the outbox returned to zero. The review changes were subsequently checked with the packaged-provider integration suite above; the standalone Compose demonstration was not repeated.

This separate executable check found and fixed a nullable NATS inactivity-threshold setting before delivery. Ten consumer configuration checks cover the fix and reject lossy consumer policies. Artifact inspection also confirms the provider contains its service registrations and migration, while Keycloak classes are not bundled and NATS/Bouncy Castle packages are isolated.

## Limits of this evidence

These are functional and failure-injection tests, not a throughput benchmark or a proof against every infrastructure failure. The broker tests use one disk-backed NATS node with `sync_interval: always`; multi-node NATS quorum failure, physical power loss, backup restore, production TLS/JWT credentials, external user storage and databases other than PostgreSQL still need deployment-specific validation. The multi-node test covers two Keycloak nodes sharing PostgreSQL and their clustered cache. No global or per-user event ordering guarantee is asserted.

GitHub Actions is configured to run the full suite for both runtime versions with Docker on Linux. That remote CI run has not been executed as part of this local build. The custom JPA API and event-listener SPI retain their upstream unsupported/internal status despite passing tests; see [compatibility](compatibility.md).
