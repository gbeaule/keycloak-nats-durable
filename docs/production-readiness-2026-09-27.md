# Production readiness review, 2026-09-27

Reviewed the transactional capture and relay, stream safety checks, shared TLS/configuration code,
consumer transactions and recovery policies, migrations, deployment examples, CI and release tools.
The four confirmed defects below are fixed. Deployment acceptance still depends on the actual
storage, credentials, capacity and recovery arrangements described in [operations](operations.md).

## Confirmed defects and fixes

| Area | Trigger and prior behavior | Fix and regression evidence |
| --- | --- | --- |
| Consumer JSON boundary | A valid envelope followed by another JSON document or garbage was processed using only its first document. Duplicate keys silently selected the last value, and numeric `specversion: 1.0` was coerced to a supported string. These deliveries could commit an effect and be ACKed. | Reject trailing tokens and duplicate keys before database access; require textual version fields. Unit regressions exercise all cases. Container regressions verify default retry keeps malformed deliveries, explicit quarantine preserves their exact bytes, and subsequent valid work proceeds. |
| Provider authentication | Leading or trailing whitespace in `KND_TOKEN` was removed by generic configuration normalization. A consumer using the same secret could authenticate while the provider accumulated an undeliverable backlog. | Preserve the token exactly. A regression checks the value, and a real Keycloak/NATS test exercises authentication and outbox delivery using a token with surrounding spaces. |
| Consumer transport configuration | The consumer accepted URL forms the provider rejected, including embedded credentials, unsupported schemes, paths and empty server entries. Address whitespace also remained unnormalized. | Both applications use the shared `NatsServers` validator. Tests cover invalid inputs, safe diagnostics, DNS/IPv4/IPv6 addresses, optional ports and defensive array copies. |
| Security gate completeness | A runtime report containing `Results: []`, empty result objects, or only OS packages was accepted as a scan with no blocking findings. | Require a populated Java package inventory in the runtime scan. Regression fixtures prove incomplete scans fail and a populated vulnerability-free inventory succeeds. The existing upstream advisory policy is preserved. |

The JSON behavior matches the explicit opt-in controls documented by
[Jackson](https://github.com/FasterXML/jackson-databind/wiki/Deserialization-Features).
Requiring scan coverage is especially relevant to externally generated inventories:
[Trivy documents limitations when consuming SBOMs produced by other tools](https://trivy.dev/docs/latest/target/sbom/).

## Verification

Before the fixes, the existing 388 Java unit tests passed. New regressions then failed for all four
findings, including four incomplete security-report fixtures.

Final verification used Java 21.0.10, Maven 3.9.6 and Docker Engine 29.8.0 on Windows 11, with
Keycloak 26.7.4, PostgreSQL 18.6 and NATS 2.15.0:

| Check | Result |
| --- | --- |
| Java unit tests | 417 passed: 40 transport, 320 provider and 57 consumer |
| Container integration tests | 48 passed, including the three new malformed-JSON/token cases |
| Python release/security-tool tests | All 5 passed |
| Checkstyle, Spotless and JAR packaging | Passed; provider relocation and absence of bundled Keycloak/test classes checked |
| Fresh aggregate SBOMs | Complete and shipped-runtime inventories regenerated in Maven online mode |
| Final Trivy gate | 0 blocking records, 30 advisory records; completed 2026-09-27 at 03:44:20 UTC |

The complete Maven integration run finished in 19 minutes 55 seconds with zero failures, errors or
skips. Coverage includes rollback, broker/process crashes, concurrent nodes, consumer transactions,
quarantine/replay, TLS rejection, quorum loss, optimized HTTPS startup, JWT rotation, coordinated
restore, PostgreSQL promotion and a persisted-outbox Keycloak 26.6.4 → 26.7.4 upgrade.

The cached-dependency verification used Maven offline mode. Because CycloneDX skips execution in
that mode, both aggregate inventories were generated separately with
`cyclonedx:makeAggregateBom@complete-inventory` and
`cyclonedx:makeAggregateBom@bundled-runtime-inventory` under `-Psbom` in online mode. The final
security scan consumed those fresh inventories and inspected the newly packaged images. Its runtime
report includes 17 Java package records; an empty runtime report is now a regression-tested failure.

JUnit reports, inventories, scan reports, build logs, artifact hashes and changed-source hashes are
preserved locally in `.work/readiness-evidence-20260927/`. The full log is
`.work/readiness-integration-20260927.log`; the final scan log is
`.work/readiness-security-final-20260927.log`.

The standalone full Keycloak 26.6.4 suite, PostgreSQL 14–17 compatibility matrix and performance
benchmark were not rerun for this patch. Earlier results remain in [testing](testing.md).

## Deployment boundaries

The local Compose stack is a demonstration with plaintext networking, development Keycloak mode,
demonstration credentials and one broker. Production needs the separate configuration in
[operations](operations.md), including durable PostgreSQL commits and replicated JetStream storage.

Delivery remains at least once, with no global or per-user ordering guarantee. Applications must
retain deduplication state and reconcile state projections. Outbox capacity, sustained throughput,
the actual ingress and credential rotation, and coordinated recovery across stores require staging
acceptance for the intended deployment. The reference tests do not establish those deployment limits.

The existing [upstream security findings and owner acceptance](security-findings.md) remain part of
the assessment. No upstream server internals were replaced and no security-policy exceptions were
added by this review.
