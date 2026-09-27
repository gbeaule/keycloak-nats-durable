# Phase 5: local audit retention and automatic cleanup

Prerequisite: [local discard](04-expiry-and-local-discard.md). Outcome: diagnostic history drains
automatically while pending publication work and per-user sequencing remain intact.

## Read first

- Phase 4's audit table and atomic discard transaction.
- `extension/src/main/java/io/github/gbeaule/keycloaknats/BridgeConfig.java`
- `DurableEventListenerFactory.java`, database transaction helpers and maintenance schema code.
- Existing managed-session, shutdown, reporting and custom-schema tests.

## Work

1. Add a configurable audit retention duration, default seven days. Allow zero for no diagnostic
   history after the next cleanup pass. Keep maintenance settings outside frozen event policies;
   changing retention may affect existing audit history without changing pending delivery policy.
2. Implement a scheduled cleanup worker using Keycloak's managed sessions/database connections.
   Use a one-minute interval and at most 500 rows per transaction as initial configurable defaults.
   Validate finite batch, interval and transaction limits.
3. Index audit discard time. Claim the oldest eligible audit rows with bounded selection and
   `SKIP LOCKED`; delete only committed audits satisfying `discardedAt + retention <= databaseNow`.
   Multiple nodes may clean safely. Do not hold a global lock or issue unbounded deletion.
4. Start/stop cleanup with provider lifecycle. An interrupted or failed cleanup transaction rolls
   back and is retried later. Cleanup failure must not stop capture or publication workers.
5. Add readonly metadata inspection and metrics for retained rows, oldest eligible age, deleted rows,
   last successful sweep and failed sweeps. Provide tests showing records actually drain.

## Storage boundaries

Every audit is final when its discard transaction commits; there is no pending audit-delivery
obligation. NATS availability and subscriber activity do not affect cleanup eligibility. Do not
create a NATS audit stream, forwarding worker or mandatory export sink.

The cleanup query touches only the diagnostic table. No cascading relationship may delete an
outbox row or reset a sequence counter when audit history expires. Audit removal must not be used
to decide whether a business event should be retried or republished.

Source counters remain compact per-user state and survive user deletion/outbox draining. Events
without a user allocate no permanent ordering counter. Pending protected events remain subject to
durability and capacity planning; they are not audit history and this worker must never purge them.

Prometheus exposes aggregate observations, not per-event audit records. Do not use event/user IDs
as metric labels. Audit cleanup must not reset running aggregate counters; normal process/monitoring
retention semantics still apply. Logs and metrics do not substitute for the atomic discard decision.

## Acceptance and validation

- Seven-day retention, exact cutoff, retention zero and changed maintenance settings behave as
  configured against database time.
- Old diagnostic records drain without any NATS connection, active subscriber or manual command.
- Audit deletion cannot cause republishing, sequence reset, lost pending work or missing payloads.
- Multiple workers, locked rows, restart and failed cleanup commit produce bounded idempotent work.
- New audits can be inserted while old audits are cleaned. Capture/publication latency is measured
  under a large audit backlog; cleanup can catch up at the expected discard rate.
- Custom database schemas and runtime permissions work without a new datasource or DDL privileges.

```text
python scripts/validate.py
python scripts/validate.py integration --maven-arg=-Dit.test=OutboxAuditRetentionIT,CustomSchemaIT
```

## Handoff

Provide cleanup settings, query/indexes, lifecycle hooks, inspection entry points and drain-test
results. Distinguish historical audit storage from protected pending work and compact counters.
