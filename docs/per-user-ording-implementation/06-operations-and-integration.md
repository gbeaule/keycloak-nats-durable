# Phase 6: producer runtime and operational integration

Prerequisite: [phases 1–5](README.md#phase-index). Outcome: the extension's ordering, publication
policy and local diagnostics are runnable and observable without a receiving application.

## Read first

- `extension/src/main/java/io/github/gbeaule/keycloaknats/BridgeConfig.java`
- `DurableEventListenerFactory.java`, `NatsDiagnostics.java` and new cleanup/expiry workers.
- Existing readonly `OutboxReport` utility, deployment configuration and monitoring rules.
- `README.md`, `docs/architecture.md`, `docs/events.md`, `docs/operations.md`, `docs/development.md`
- Packaging and production/readiness integration tests.

## Work

1. Wire ordered relay, source expiry scanning and local audit cleanup into startup/shutdown.
   Stop new claims and drain bounded work before releasing resources; restart rediscovers pending
   originals without a consumer database or subscription.
2. Expose bounded producer concurrency, scan and retention settings with existing configuration
   precedence. Keep the event publication policy in the extended filter file. No consumer rule,
   receiver expiry budget or application-processing setting belongs in that file.
3. Extend producer metrics and readonly reports for original backlog, per-user blocked heads,
   publication failures, local discards and audit cleanup. A reused reporting utility must remain
   independently usable without running its package's optional consumer program.
4. Add an example filter with explicitly discardable ephemeral events and protected lifecycle
   events. Avoid prescribing a business-critical deletion as discardable by default.
5. Update provider schemas, monitoring grants and packaging as needed. Add no runtime dependency
   on `consumer-example`, no consumer provisioning requirement and no audit publisher.
6. Keep existing ordinary Keycloak-event subject routing. Require no full-feed subscriber, control
   subject or special receiver library. Do not change application ACK, inbox or effect processing.

## Observability

| Signal | Meaning |
| --- | --- |
| Original backlog and oldest capture age | Pending publication work, including protected events. |
| Blocked users and retry activity | Source sequencing/backoff, not downstream application health. |
| Discard counts by bounded reason | Committed local abandonment under configured publication policy. |
| Publication outcome uncertainty | A dropped original may previously have reached NATS. |
| Audit rows/eligible age and cleanup status | Historical diagnostic retention and drain health. |

Use bounded metric labels and safe diagnostic codes. Readonly audit inspection answers what was
dropped, under which rule, why and when without showing the original payload. Prometheus contains
aggregate measurements; PostgreSQL contains the seven-day per-event diagnostic history.

An empty outbox establishes that the extension has resolved publication obligations, not that any
application received or processed events. Do not monitor consumer cursors or application effects as
part of extension health. Shared broker/database failures can still affect all users.

## Documentation

Describe only the publisher contract and its operational consequences:

- Accepted events and their per-user ordering metadata; nested target attribution and userless events.
- Publication selection, retries and local discard; valid sequence gaps and unchanged subjects.
- Capture timestamp versus source event timestamp; policy snapshots survive reloads.
- Successful NATS acceptance ends publication responsibility. Subscriber processing is external.
- A timeout is not proof of non-publication. Local discard stops retries without retracting an
  accepted or in-flight original. Audit language must reflect that uncertainty.
- Local diagnostic storage, seven-day automatic retention, cleanup health and compact counter state.
- No old-data migration for this unreleased feature; no automatic deletion of developer volumes.

Keep exact names/defaults in code and schemas. The existing consumer example may remain an optional
illustration, but do not expand it or present it as required for this feature. Adjust only fixtures
that must recognize the revised business-event schema; consumer cleanup/removal is separate work.

## Acceptance and validation

- A fresh Keycloak/provider and NATS deployment works with no consumer application or database.
- Shutdown/restart during publish, local discard and audit cleanup preserves pending obligations.
- A broker outage does not prevent already-eligible local discard or audit cleanup from completing.
- Only original Keycloak-derived messages appear in NATS; no control or audit messages are emitted.
- Readonly diagnostics and aggregate metrics agree with committed source state and report cleanup
  failure. They do not claim downstream completion.
- Installation/build artifacts still contain no consumer application dependency.

```text
python scripts/validate.py
python scripts/validate.py integration --maven-arg=-Dit.test=ProductionModeIT,NatsReadinessIT,PerUserPublicationIT
```

## Handoff

Provide producer settings, standalone smoke-test steps/results, reporting/cleanup entry points and
updated architecture documentation. Phase 7 verifies the source boundary without a consumer framework.
