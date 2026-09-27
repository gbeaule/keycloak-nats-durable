# Phase 7: system verification and release readiness

Prerequisite: [phases 1–6](README.md#phase-index). Outcome: evidence that the composed feature
preserves order and explicit discard semantics under concurrency, restarts and uncertain outcomes.
Do not mark the feature complete solely because component tests pass.

## Read first

- `integration-tests/src/test/java/io/github/gbeaule/keycloaknats/IntegrationSupport.java`
- `DurabilityIT`, `FilteringIT`, `ConsumerHardeningIT`, `NatsClusterIT`, `PostgresRecoveryIT`,
  `CustomSchemaIT`, `ProductionModeIT`, `UpgradeIT` and `ThroughputBenchmark` in that module.
- New capture, relay, receipt and scheduler tests from phases 2–5.
- `scripts/validate.py`, `scripts/test_validation.py`, `config/keycloak-versions.json`
- `.github/workflows/verify.yml`, the Maven integration/performance profiles and release tooling.

## System test matrix

Use deterministic latches, controlled transactions and explicit fault hooks where possible.
Record committed business-effect order, terminal outcomes, source audits, pending notices and
consumer cursors; broker delivery order alone cannot establish the guarantee.

| Scenario | Required assertion |
| --- | --- |
| Concurrent same-user capture on two Keycloak nodes | Consecutive committed sequence; no event from a rolled-back transaction. |
| Multiple events in one transaction | Callback order retained and all-or-nothing counter/event persistence. |
| Direct and nested admin changes plus user events | One target-user sequence; actor identity never changes the key. |
| Same user ID in different realms; missing user ID | Independent keys and no global unknown-user bottleneck. |
| Retained head locked or awaiting retry | Its successor cannot resolve; other users progress in both relay and consumer. |
| Capture clock skew versus event timestamp | Sequencing follows database serialization; expiry uses captured age. |
| Rule overlap/reload during capture and backlog | First-match behavior and frozen original policy across both stages. |
| Capture exclusion | No sequence allocated and no downstream gap. |
| Outbox age/failure limit | Audit plus persistent notice, no payload, successor progresses only after notice obligation resolves. |
| Consumer age/failure limit | Audit and ordered terminal cursor; no effect for the discarded sequence. |
| Both thresholds, equality, no thresholds | OR behavior, exact boundary and protected default. |
| Non-head expiry | Payload cleared with pending decision; no cursor or publication jump over predecessors. |
| Original accepted, ACK lost, then discard | Original and notice can coexist; at most one terminal result/effect. |
| Notice accepted, ACK or database commit lost | Same notice identity retried; no lost ordering obligation. |
| Delayed stale publisher after discard | Late original cannot resurrect a terminal slot or regress its cursor. |
| Out-of-order receipt | Later sequences buffer durably; no initialization from the highest seen value. |
| Missing predecessor or invalid notice | Only that user's ordered progress waits; no time-based gap skipping. |
| Receipt committed before ACK failure | Duplicate receipt resolves by hash/identity; no duplicate effect. |
| Receipt ACKed before process crash | Inbox alone recovers pending business work. |
| Consumer effect commit uncertain | Next owner reconciles terminal state; no repeated or reordered effect. |
| Eligible handler rejection versus infrastructure error | Only the deliberate confirmed rejection increments the failure-limit counter. |
| Handler timeout/failed rollback/unresponsive task | No late commit, unsafe discard or cursor advance. |
| Duplicate after broker dedup window/payload cleanup | Evidence still deduplicates without restoring discarded bytes. |
| Conflicting original/notice reference or duplicate bytes | Conflict reported; no arbitrary replacement or new effect. |
| Local business filter | Ordered ignored disposition; original and notice both durably received. |
| Two logical applications | Shared source sequence but independent effects, local discard and cursors. |
| Narrow broker filter or fresh DB on incomplete history | Invalid configuration rejected or gap reported; no false completeness claim. |
| Broker/DB restart, NATS leader change, custom DB schema | Pending originals/notices/receipts recover with identities and outcomes intact. |

An already-terminal identity conflict must preserve the existing committed result and report the
conflict; no implementation can undo a previously committed effect by discovering bad input later.
For unresolved slots, conflict evidence must prevent terminal processing until repaired.

## Failure injection and invariants

Exercise every durable boundary with actual PostgreSQL and JetStream, including independent
workers and multiple Keycloak nodes. Mocks are useful for classification but insufficient for
transaction visibility, lock ownership, uncertain publication or duplicate persistence behavior.

At the end of each recovery scenario, account for every accepted capture: it is an unresolved
outbox obligation, a published original/notice, a durable pending receipt, or a terminal outcome for
each intended consumer. The same sequence can have an original and notice in transport, but cannot
have two different terminal business outcomes within one consumer.

Audit failures must not remove payloads or move cursors. A failure to persist a notice must leave
the original publication obligation intact. A failed notice publication must retain its committed
metadata even if the original business payload has already been discarded.

Do not assume source discard guarantees every consumer discarded: a consumer can have applied the
original before the discard decision became visible. Assert the documented first-terminal-wins
behavior rather than inventing distributed rollback.

## Compatibility and baseline changes

Add the new transaction/locking/schema tests to `POSTGRES_TESTS` in `scripts/validate.py` and update
its script tests so PostgreSQL matrix jobs actually run them. Run the configured Keycloak runtime
matrix for listener paths, nested resource attribution and packaging.

Remove only assertions that require the obsolete unreleased schema/format. Keep fresh-install,
restart, custom-schema, durable-commit, TLS, transaction timeout and Keycloak runtime compatibility
coverage. No old-data migration or mixed-feature-version test is required. If runtime-upgrade tests
persist data, both sides must use the new feature schema and an appropriate provider build.

## Performance and capacity

Extend the existing benchmark to measure the new costs without asserting workstation-specific
throughput thresholds. Compare runs on the same infrastructure and record configuration:

- Capture latency and database waits with mostly independent users versus one hot user.
- Relay throughput and fairness with retained failures, expiring events and pending notices.
- Receipt rate, ACK latency, processing rate and inbox growth as separate measurements.
- Progress of healthy users while one user's handler is slow or retained indefinitely.
- Expiry-scan and head-selection query behavior with large backlogs; inspect query plans and index
  usage, including many ineligible heads.
- Recovery throughput while new events arrive, database connection use, audit/notice/counter
  storage growth and time to drain.

Per-user capture serialization and durable receipt add real database work. Report observed costs
and capacity limits; do not hide them by weakening sequencing or reverting to shared partitions.

## Validation commands

Run from the repository root with the project's required JDK, Maven, Python and Docker available:

```text
python scripts/validate.py
python scripts/validate.py integration
python scripts/validate.py matrix
python scripts/validate.py security
mvn -B -ntp -Pperformance verify
```

Use focused runs while developing, then run these final gates on one coherent revision. Security
validation is relevant to the added shared module and packaging changes. Existing reports from
before this feature do not validate the new artifacts. Record actual command results and any
environmental limitations; do not silently omit container tests when Docker is unavailable.

## Completion and handoff

- Every agreed decision in the index has executable coverage or a stated architectural boundary.
- Fresh demo runs demonstrate ordered effects, independent users, retained lifecycle events and
  explicitly discardable events advancing through audit/notice handling.
- No production code path can skip an unknown sequence, discard a notice or advance without a
  committed effect/ignore/discard disposition.
- Operational documentation matches actual schema and commands and distinguishes receipt from
  completion. No unreleased compatibility converter was introduced.
- Deliver a concise report with changed entry points, exact validations, test/report locations,
  benchmark observations and unresolved issues. Publish or deploy only under a separate request.
