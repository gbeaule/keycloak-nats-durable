# Verification record

Verified locally on 2026-09-24 using Windows 11, Docker Desktop / Docker Engine 29.8.0, Maven 3.9.6 and IntelliJ's Java 21.0.10 runtime. Tests use Keycloak 26.7.4, PostgreSQL 17.6 and NATS 2.12.8. Testcontainers 2.0.5 manages isolated containers and cleans them up.

| Suite | Passed | Failed | Skipped |
|---|---:|---:|---:|
| Extension unit tests | 57 | 0 | 0 |
| Consumer configuration unit tests | 10 | 0 | 0 |
| Real-service integration tests | 15 | 0 | 0 |
| Total | **82** | **0** | **0** |

Commands used: `mvn -B -ntp -Pintegration verify` for the full integration run, and `mvn -B -ntp -pl consumer-example -am verify` after the consumer startup regression fix. Local runs set Java 21 in `JAVA_HOME` and used a workspace-local Maven cache. `spotless:check` passes; `spotless:apply` maintains formatting.

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
13. Two real Keycloak nodes drain concurrent account events with unique transport identities.
14. A hard NATS restart preserves pending messages and durable acknowledgement state.
15. Replay after JetStream's deduplication window expires still produces one consumer database effect.

See [DurabilityIT](../integration-tests/src/test/java/io/github/keycloaknats/DurabilityIT.java) and the [test infrastructure](../integration-tests/src/test/java/io/github/keycloaknats/IntegrationSupport.java). Tests do not use `disabledWithoutDocker` or silently skip unavailable infrastructure. Read JUnit XML under `integration-tests/target/failsafe-reports` and logs under `integration-tests/target` for a local run's evidence.

## Packaged application checks

The Docker Compose images were built and run from the generated JARs. Re-running provisioning against the same stream/consumer succeeded. The executable consumer processed a real login and temporary account create/update/delete exactly once into `knd_effects`; the outbox reached zero. During a second login with NATS stopped, the outbox retained one row. After restart, the same running consumer recovered, the effect count increased by one, and the outbox returned to zero.

This separate executable check found and fixed a nullable NATS inactivity-threshold setting before delivery. Ten consumer configuration checks cover the fix and reject lossy consumer policies. Artifact inspection also confirms the provider contains its service registrations and migration, while Keycloak classes are not bundled and NATS/Bouncy Castle packages are isolated.

## Limits of this evidence

These are functional and failure-injection tests, not a throughput benchmark or a proof against every infrastructure failure. The broker tests use one disk-backed NATS node with `sync_interval: always`; multi-node NATS quorum failure, physical power loss, backup restore, production TLS/JWT credentials, external user storage and databases other than PostgreSQL still need deployment-specific validation. The multi-node test covers two Keycloak nodes sharing PostgreSQL and their clustered cache. No global or per-user event ordering guarantee is asserted.

GitHub Actions runs the full suite with Docker on Linux when this workspace is pushed to a repository; that remote CI run has not been executed as part of this local build.
