# Verification record

Verified locally on 2026-09-25 using Windows 11, Docker Desktop / Docker Engine 29.8.0, Maven 3.9.6 and IntelliJ's Java 21.0.10 runtime. Tests use Keycloak 26.6.4 and 26.7.4, PostgreSQL 17.6 and NATS 2.12.8. Testcontainers 2.0.5 manages isolated containers and cleans them up.

## PEM library follow-up

The latest change replaces manual private-key PEM decoding with Bouncy Castle LTS 2.73.13, aligning `bcprov`, `bcutil` and `bcpkix`. Maven Shade 3.6.2 handles the dependency's newer multi-release class files. TLS remains optional, and the default capture policy still includes all events.

All **323 unit tests** pass: 12 transport, 301 extension and 10 consumer tests. The transport cases perform mutual-TLS handshakes and exchange application data using RSA and EC PKCS#8, RSA PKCS#1 and EC SEC1 keys. They also reject malformed, encrypted, multiple, public-only, empty and oversized keys, and check plaintext/default-trust configuration. Certificates and keys are generated for the tests.

Targeted integration verification passed on both supported Keycloak runtimes. It runs the three existing NATS mTLS/HA scenarios and a new plaintext scenario that publishes with no consumer defined, then creates a consumer and verifies delivery.

| Runtime | Unit tests passed | Selected integration tests passed | Failures/errors | Skipped |
|---|---:|---:|---:|---:|
| Keycloak 26.6.4 | 323 | 4 | 0 | 0 |
| Keycloak 26.7.4 | 323 | 4 | 0 | 0 |

Both runs passed packaging, Spotless and Checkstyle. The command is:

```sh
mvn -B -ntp -Pintegration -Dkeycloak.runtime.version=26.6.4 \
  '-Dit.test=NatsClusterIT,DurabilityIT#messagesWaitForConsumerCreatedAfterPublication' verify
```

Use `26.7.4` for the other runtime. Each command also reruns all 323 unit tests, packaging, Spotless and Checkstyle. The earlier full matrix below predates this PEM refactor; the complete integration suite has not been rerun for this follow-up. CI's unfiltered integration command includes the new retention test.

Reports and artifact SHA-256 hashes are archived in `.work/pem-evidence/keycloak-26.6.4/` and `.work/pem-evidence/keycloak-26.7.4/`, with Maven logs at `.work/verified-pem-26.6.4.log` and `.work/verified-pem-26.7.4.log`. These archives include only reports from the selected integration cases, avoiding stale reports from prior full runs. Packaging inspection confirms both JARs contain the PEM parser, the provider relocates Bouncy Castle, and neither JAR bundles Keycloak or Liquibase classes.

## Full matrix before the PEM follow-up

| Runtime | Extension tests | Consumer tests | Integration tests | Total passed | Failures/errors | Skipped |
|---|---:|---:|---:|---:|---:|---:|
| Keycloak 26.6.4 | 301 | 10 | 25 | **336** | 0 | 0 |
| Keycloak 26.7.4 | 301 | 10 | 25 | **336** | 0 | 0 |

Both earlier runs passed the complete suite, Spotless formatting and the full Google Checkstyle ruleset, including mandatory braces, in approximately ten minutes each. These were 336 distinct test cases repeated on two runtimes, for 672 passing executions. Local commands set Java 21 in `JAVA_HOME` and used a workspace-local Maven cache:

```sh
mvn -B -ntp -Pintegration -Dkeycloak.runtime.version=26.6.4 verify
mvn -B -ntp -Pintegration -Dkeycloak.runtime.version=26.7.4 verify
```

Both runtime targets use provider code compiled against the same Keycloak 26.7.4 API baseline; this is runtime compatibility testing, not just separate successful compilations. Candidate runs are available through the `verify` workflow's optional `keycloak_version` input; see [the compatibility process](compatibility.md). The existing-database replacement case starts on 26.6.4 with pending events: the 26.7.4 target tests an upgrade, and the 26.6.4 target tests same-version reinstallation.

Event coverage includes all 132 user event enums, all 39 admin resource enums paired with all four operation enums, null-omission/privacy rules, custom resource routing, twelve schema-validated JSON examples and a catalogue checked against the actual compiled enums. The admin resource combinations run inside parameterized cases and are not inflated into separate test counts.

Capture-policy tests cover strict validation, wildcard/exact selection, disabled-user observations, custom resources, initial-file rejection, last-good-policy retention and content changes with unchanged timestamps. Configuration tests reject mixed TLS/plaintext endpoints and incomplete client credentials. Excluded events avoid both persistence and unnecessary user lookups.

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
20. PostgreSQL table and TOAST autovacuum settings are installed. A trigger rejects any UPDATE mentioning payload, subject or capture time; several failed delivery attempts update only metadata, and recovery delivers the original large payload before removing its row.
21. A running Keycloak process switches capture policies by atomic file replacement while NATS is down. Disabled-only selection takes effect, malformed replacements retain the last valid policy, and a later exclude-all policy leaves previously captured IDs intact for delivery.
22. A real mTLS NATS cluster rejects an untrusted server certificate, a mismatched server hostname and a client without its required certificate.
23. Two Keycloak nodes in the same cache cluster concurrently capture 24 operations into a three-replica JetStream stream. After four confirmed consumer ACKs, kill the actual stream leader; the remaining 20 unique events survive, acknowledged IDs do not return, and new capture succeeds with one broker down.
24. Lose two of three NATS brokers, capture three operations, and verify persisted retries. Kill one Keycloak node, restore broker quorum, and require the surviving Keycloak node to deliver exactly the original pending IDs.
25. Initialize the extension in a quoted, non-public PostgreSQL schema, verify its maintenance settings and absence from `public`, then hard-restart Keycloak with NATS down and recover the retained event.
26. Publish over plaintext NATS with no consumer defined, verify the outbox drains into the stream, create a consumer afterward and verify the retained message is delivered and removed only after ACK. Added in the PEM follow-up.

See [DurabilityIT](../integration-tests/src/test/java/io/github/keycloaknats/DurabilityIT.java), [UpgradeIT](../integration-tests/src/test/java/io/github/keycloaknats/UpgradeIT.java), [FilteringIT](../integration-tests/src/test/java/io/github/keycloaknats/FilteringIT.java), [NatsClusterIT](../integration-tests/src/test/java/io/github/keycloaknats/NatsClusterIT.java), [CustomSchemaIT](../integration-tests/src/test/java/io/github/keycloaknats/CustomSchemaIT.java) and the [test infrastructure](../integration-tests/src/test/java/io/github/keycloaknats/IntegrationSupport.java). Tests do not use `disabledWithoutDocker` or silently skip unavailable infrastructure. Read JUnit XML under `integration-tests/target/failsafe-reports` and logs under `integration-tests/target` for a local run's evidence. Upgrade source and target logs have separate filenames.

The cluster tests generate temporary certificates and protect both client and broker-route connections with mutual TLS. They wait for stream and durable-consumer leadership independently: those Raft groups recover separately. Confirmed ACKs are followed by an eventual stream-removal check, because WorkQueue deletion can trail the consumer ACK. These readiness checks preserve the exact-ID, message-count and retained-outbox assertions.

Maven replaces `target` reports on subsequent runs. Final reports, logs and provider/consumer SHA-256 hashes are preserved under `.work/feature-evidence/keycloak-26.6.4/` and `.work/feature-evidence/keycloak-26.7.4/` (workspace-only, not committed). Full Maven logs are `.work/verified-features-26.6.4.log` and `.work/verified-features-26.7.4.log`. CI uploads a separate artifact for each runtime version. During the earlier compatibility review, the workflow YAML was parsed locally and its actual matrix-generation script checked with seven inputs, including valid, duplicate and invalid release values; extra Maven arguments and shell syntax were rejected.

## Packaged application checks

During the initial implementation, the Docker Compose images were built and run from the generated JARs. Re-running provisioning against the same stream/consumer succeeded. The executable consumer processed a real login and temporary account create/update/delete exactly once into `knd_effects`; the outbox reached zero. During a second login with NATS stopped, the outbox retained one row. After restart, the same running consumer recovered, the effect count increased by one, and the outbox returned to zero. The review changes were subsequently checked with the packaged-provider integration suite above; the standalone Compose demonstration was not repeated.

This separate executable check found and fixed a nullable NATS inactivity-threshold setting before delivery. Ten consumer configuration checks cover the fix and reject lossy consumer policies. Artifact inspection confirms the shared TLS and hostname-verification classes are included in both executable JARs, while Keycloak and Liquibase classes are not bundled. The provider retains its service registrations and migration; its NATS/Bouncy Castle packages are isolated.

## Limits of this evidence

These are functional and failure-injection tests, not a throughput benchmark or a proof against every infrastructure failure. They use disk-backed NATS with `sync_interval: always`, including a three-broker replicated cluster, and two Keycloak nodes sharing PostgreSQL and their clustered cache. Physical power loss, actual PostgreSQL HA failover, backup restore across stores, sustained production load, deployment-specific certificates/JWT permissions and external/federated user storage still need validation in the target environment. Databases other than PostgreSQL are outside the supported scope. No global or per-user event ordering guarantee is asserted.

GitHub Actions is configured to run the full suite for both runtime versions with Docker on Linux. That remote CI run has not been executed as part of this local build. The custom JPA API and event-listener SPI retain their upstream unsupported/internal status despite passing tests; see [compatibility](compatibility.md).
