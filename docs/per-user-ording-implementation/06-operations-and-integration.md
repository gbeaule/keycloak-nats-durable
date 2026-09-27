# Phase 6: runtime integration and operations

Prerequisite: [phases 1–5](README.md#phase-index). Outcome: a runnable demo and an operable feature
whose health, backlog and discard decisions can be understood without reading payloads.

## Read first

- `extension/src/main/java/io/github/gbeaule/keycloaknats/BridgeConfig.java`
- `consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/ConsumerConfig.java`
- `ConsumerSettings.java`, `ConsumerMain.java`, `ConsumerMonitor.java`, `OutboxReport.java` and
  `QuarantineStore.java` in the consumer package.
- `compose.yaml`, `deploy/postgres-init.sql`, `deploy/prometheus-rules.yml`, `deploy/Dockerfile.*`
- `README.md`, `docs/architecture.md`, `docs/events.md`, `docs/operations.md`, `docs/development.md`
- Existing config/reporting/monitoring tests and packaging/release scripts.

## Work

1. Wire capture, ordered relay, expiry scans, durable receipt and processing workers into startup and
   shutdown. Stop new claims, allow bounded work to finish, then release connections; restart must
   rediscover all committed pending work. A publisher or scheduler failure must remain observable.
2. Expose bounded settings for receipt concurrency, processing concurrency, polling, expiry scanning
   and database capacity. Preserve existing configuration precedence. Reject resource combinations
   that cannot leave receipt capacity while handlers use their permitted connections.
3. Keep delivery policy in the extended `KND_FILTER_FILE` document and in captured envelopes. Remove
   consumer environment overrides that could silently reinterpret its frozen policy, including
   the old failure action, minimum delivery count, failure subject and drop-age controls. Reject
   supplied retired settings with a concise configuration error rather than silently ignoring them.
4. Remove the old metadata-or-payload quarantine model, quarantine commands and original replay
   path from the example. Replace them with metadata-only audit/backlog inspection. Retire related
   tests deliberately, replacing their durability coverage with the new receipt/outcome scenarios.
   No manual skip or replay command is part of this release.
5. Wire optional local business subject selection after durable receipt. Freeze its decision per
   original receipt and document that all replicas of a logical consumer share the same selection.
   Control notices remain mandatory input and never invoke a business handler.
6. Update provisioning to create a full-feed durable suitable for independent ingestion and
   processing. Preserve file-backed streams, explicit ACKs, no stream TTL/eviction and correct
   WorkQueue versus independent-application Limits retention semantics.
7. Extend readonly outbox reports and consumer health/metrics for the new tables/states. Update
   monitoring grants, fresh consumer DDL, schema-presence checks and demo defaults together.
8. Add an example extended filter showing ephemeral events eligible for discard and lifecycle
   events retaining the default. Do not make the demo discard all events implicitly. Include a
   small runnable scenario showing one blocked user, another progressing, then permitted discard
   releasing the first user.
9. Verify the new shared module is included in both shaded artifacts and the relevant inventories.
   Update build/release assumptions that enumerate modules; do not publish a release as part of
   this implementation phase.

## Observability contract

Distinguish durable state from process-local counters. Expose backlog/age gauges from committed
database state so restarts do not make the system appear drained. Use bounded labels such as stage,
reason and disposition; do not label metrics by user, event UUID or arbitrary rule IDs.

| Area | Required signals |
| --- | --- |
| Capture | Counter/lock failures and request impact; failures still roll back capture. |
| Relay | Pending original count/age, pending notice count/age, retry activity, blocked keys and discard decisions. |
| Receipt | Received/duplicate/conflicting/invalid inputs, ACK failures, inbox storage/backpressure and receipt health. |
| Processing | Pending heads, missing-sequence gaps, oldest blocked age, eligible failures, applied/ignored/discarded outcomes and worker health. |
| Audit | Discards by stage/reason, notice publication unresolved/resolved, late originals suppressed and notices observed after an effect. |

Inspection commands should show key/sequence, original identity, capture/decision time, rule/policy,
reason and whether the ordering obligation remains pending. A committed non-head discard decision
must not be displayed as a cursor advance. Do not expose payloads, arbitrary headers or sensitive
exception text in logs, reports or audit output.

Differentiate NATS receipt health from effect-processing lag. An empty JetStream pending count can
coexist with a large durable application backlog; an empty producer outbox does not prove that
business effects finished. Alerts must include pending notices, persistent gaps and collection errors.

## Documentation changes

Keep user-facing documentation architectural. Link exact schema/config/code locations for details.
Update the current claims that the outbox never expires and the bridge offers no per-user ordering.
Explain the actual ordered-effects guarantee, per-user isolation and default retention, including:

- Original `time` versus capture time and sequence order; recognized nested user identity.
- Full-feed ingestion, local filtering, sequence-1 bootstrap and missing-history limitations.
- ACK after durable receipt transfers responsibility to the consumer database.
- Metadata-only discard and the need to publish a non-expiring notice before relay advancement.
- Original/notice ambiguity: abandonment cannot undo an already-committed downstream effect.
- Source-wide discard notices versus consumer-local discard outcomes.
- Retention of counters/cursors/identity evidence, storage growth and coordinated restore of the
  consumer inbox as well as effects.
- Broker/global database outage versus a single user's application failure; isolation cannot
  eliminate shared capacity constraints.
- Unreleased schema reset: fresh installations only, no compatibility migration or automatic
  deletion of existing developer volumes. Update development guidance accordingly for this change.

## Acceptance and validation

- Fresh provisioning and initialization run without manual database edits. Runtime identities need
  only their intended DML/transport permissions; report identities remain readonly.
- Shutdown during receipt, application, original publication and notice publication recovers all
  obligations after restart.
- Unsafe/narrow durable configuration and retired policy settings fail clearly before consumption.
- Metrics distinguish queued payloads, notices, pending discard decisions and terminal outcomes.
  Bounded-label and metadata-redaction tests remain meaningful.
- Packaged provider and consumer load the shared contract successfully; independent applications
  do not share consumer cursor or local failure state accidentally.
- Run quick checks, focused production/readiness tests and a fresh Compose smoke test. Coordinate
  integration test names with phase 7; do not use volume deletion as an automatic validation step.

```text
python scripts/validate.py
python scripts/validate.py integration --maven-arg=-Dit.test=ProductionModeIT,NatsReadinessIT,ConsumerHardeningIT,PerUserOrderingIT
```

## Handoff

Provide runtime/config entry points, audit inspection commands, final architecture documentation,
smoke-test steps/results and remaining capacity concerns. Phase 7 needs a runnable feature with
operational signals, not only isolated library tests.
