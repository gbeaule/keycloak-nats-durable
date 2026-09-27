# Phase 7: publisher system verification

Prerequisite: [phases 1–6](README.md#phase-index). Outcome: evidence for source sequencing,
policy-authorized local discard, bounded diagnostics and the publisher-only runtime boundary.

## Read first

- `integration-tests/src/test/java/io/github/gbeaule/keycloaknats/IntegrationSupport.java`
- Existing durability, filtering, NATS-cluster, database-recovery, production and custom-schema tests.
- New capture, relay, discard and retention tests from earlier phases.
- `scripts/validate.py`, `scripts/test_validation.py`, compatibility configuration and the benchmark.

## Test matrix

Use actual PostgreSQL and JetStream, controlled transactions and fault injection. A test may inspect
stored NATS messages; it must not need a consumer application's business logic to prove the feature.

| Scenario | Required evidence |
| --- | --- |
| Same-user capture on multiple nodes | Stable sequence, serialized commit visibility and rollback safety. |
| Multiple callbacks in one transaction | Consistent callback order and all-or-nothing capture. |
| User plus direct/nested admin events | One affected-user ordering key, distinct from the admin actor. |
| Different realms/users and missing identity | Independent work; no global unknown-user lock or permanent per-event counter. |
| Capture filter exclusion | No outbox record or sequence allocation. |
| Rule overlap and reload | First match; the original policy stays frozen in existing backlog. |
| Locked or delayed A1 | A2 is not claimed; unrelated B1 proceeds. |
| Normal confirmed publication | Each later user's position starts after committed predecessor resolution. |
| Explicit age/failure discard | Local audit and outbox deletion commit atomically; next position becomes eligible. |
| Sequence 11 discarded | 10 and 12 may publish, with no gap-filling message. |
| Protected defaults | Old or repeatedly failing originals remain pending. |
| Non-head expiry | Eligible payload leaves the outbox without bypassing a lower retained predecessor. |
| Discard while NATS is down | Completes locally; no deferred audit-publication obligation exists. |
| Audit insertion failure or discard commit rollback | Original remains pending; no unsafe head advancement. |
| Send intent committed before crash | Outcome remains conservatively ambiguous, even with zero committed failure count. |
| Original accepted but ACK lost | Retry identity is stable; local discard never claims proven non-delivery. |
| Stale in-flight publish after local discard | No later application/protocol compensation is invented; documented uncertainty remains visible. |
| Failed database removal after PubAck | Original can retry with identical bytes and ID. |
| Seven-day, zero and exact-boundary retention | Diagnostic history drains automatically from PostgreSQL. |
| Concurrent cleanup, restart and failed commit | Bounded safe deletion; pending originals and counters remain untouched. |
| Delayed audit inspection after retention | Absence of expired history cannot trigger retry, sequence reset or another publication. |
| No receiver application deployed | Capture, publication, discard and audit cleanup all operate normally. |
| Message inventory | Every published business record derives from a captured Keycloak event; no audit/control types. |
| Custom schema and broker/database restart | Pending identities and source decisions recover correctly. |

Do not assert unconditional subscriber arrival order after ambiguous discard. The feature sequences
publication work; it cannot recall in-flight data, eliminate duplicates or control applications.
Explicitly test this limit rather than silently weakening the test setup to avoid it.

## Compatibility and capacity

Add new source transaction/locking/retention tests to the PostgreSQL selections in
`scripts/validate.py` and its script tests. Preserve unrelated existing coverage; removing consumer
requirements from this feature is not permission to disable repository tests.

Run the configured Keycloak runtime matrix for listener behavior, nested resource paths and
packaging. Fresh schemas may replace unreleased schema assertions; no old-feature migration test
is needed. Keep durable-commit, runtime restart, TLS, custom-schema and transaction timeout coverage.

Extend the benchmark to measure capture waits for a hot user versus many users, relay fairness,
recovery during new capture, expiry/index efficiency, extra send-intent transaction cost and audit
cleanup catch-up rate. Report environment and observed costs rather than workstation-specific
throughput promises. Confirm diagnostic storage reaches a steady range under sustained discard
when cleanup capacity exceeds the incoming rate. Protected outbox growth is a separate capacity issue.

## Validation

```text
python scripts/validate.py
python scripts/validate.py integration
python scripts/validate.py matrix
```

Run `python scripts/validate.py security` when dependency/packaging changes require it, and
`mvn -B -ntp -Pperformance verify` for the benchmark. Use one coherent source revision for final
results. Report missing Docker/JDK or failed checks; do not describe skipped checks as passing.

## Completion and handoff

- Only Keycloak-derived event messages are published; audit diagnostics stay in source PostgreSQL.
- No extension path depends on a consumer application, full-feed subscription, cursor or effect.
- Per-user publication selection and local discard atomicity have real concurrency/crash coverage.
- Audit history drains after seven days by default with observable bounded cleanup.
- Ordering/uncertain-publication limits are explicit; no consumer-order guarantee is claimed.
- Supply actual validations, report locations, benchmark observations and unresolved concerns.
  Deploying or publishing a release is not part of this phase.
