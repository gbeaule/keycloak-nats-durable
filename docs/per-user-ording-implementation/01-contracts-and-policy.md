# Phase 1: delivery contracts and filter policy

Prerequisite: [index and agreed decisions](README.md). Outcome: one executable contract shared by
capture, relay and consumer, with deterministic policy selection.

## Read first

- `extension/src/main/java/io/github/gbeaule/keycloaknats/EventFilter.java`
- `extension/src/main/java/io/github/gbeaule/keycloaknats/CaptureScope.java`
- `extension/src/main/java/io/github/gbeaule/keycloaknats/ReloadingEventFilter.java`
- `extension/src/main/java/io/github/gbeaule/keycloaknats/EventEnvelope.java`
- `schemas/event-v1.schema.json`, `schemas/keycloak-catalogue-26.7.4.json`, `config/events-*.json`
- `consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/FailurePolicy.java`
- Root and module POMs, including shading and `coverage/pom.xml`.

## Work

1. Introduce a small `event-contract` Maven module for shared wire metadata, resolved stage policies,
   strict parsing and validation. It must not depend on Keycloak, JPA or consumer persistence. Use
   the project's existing Jackson dependency; keep Keycloak enum matching in the extension. Wire
   the module into the reactor, extension, consumer, coverage and packaged artifacts.
2. Add a JSON schema for the filter file. Preserve existing capture-selection behavior and fields.
   Add an optional `delivery.rules` array; capture selection still runs before delivery matching.
   A delivery rule must never implicitly capture an otherwise excluded event.
3. Compile capture selection and delivery rules into one immutable snapshot. Each listener callback
   reads that snapshot once, including admin preselection and enabled-state matching. Invalid
   startup files fail startup; invalid reloads retain the entire last valid snapshot.
4. Resolve stage policies once per accepted event, with policy digest and rule ID. Persist the
   result in later phases; relay and consumer must not reread the filter to reinterpret an event.
5. Add the business delivery metadata and discard-notice schema defined in the index. Supply
   serialization fixtures and shared round-trip validators for both. Revise the unreleased v1
   contract in place; no old-envelope compatibility mode is needed.
6. Extend fixtures and catalogue/schema tests to cover nested user attribution without changing
   `userEnabled` into a transition or extending that observation to nested operations.

## Rule semantics

Rules have unique nonblank IDs, a `match` object and optional `outbox` and `consumer` stage policies.
Evaluate in file order; the first matching rule supplies both stages. An omitted stage means retry
forever. No matching rule, omitted `delivery`, or an empty rules array means retry forever in both
stages. Do not merge several matching rules or infer specificity.

`match.kind` is required and is `user` or `admin`. User rules require `eventTypes`; admin rules
require `resourceType` and `operations`. Reuse existing enum/custom-resource/wildcard semantics.
Optional realm, client, outcome and subject selectors use the existing scope semantics. Optional
`userEnabled` has the existing direct-USER CREATE/UPDATE restriction and cannot match an error
event. Reject fields belonging to the other kind, unknown fields, duplicate IDs/JSON keys,
contradictory selectors and invalid types.

Example configuration shape to encode in the schema and parser tests:

```json
{
  "userEvents": ["*"],
  "adminEvents": [{"resourceType": "*", "operations": ["*"]}],
  "delivery": {
    "rules": [
      {
        "id": "short-lived-login",
        "match": {"kind": "user", "eventTypes": ["LOGIN", "LOGIN_ERROR"]},
        "outbox": {"action": "discard", "maxAgeSeconds": 300, "maxFailures": 20},
        "consumer": {"action": "discard", "maxAgeSeconds": 900, "maxFailures": 5}
      }
    ]
  }
}
```

The numbers are illustrative, not recommended deployment limits. With this example, unmatched
events, including user lifecycle admin events, retain the no-discard default.

`retry` forbids thresholds. `discard` requires at least one threshold. Thresholds are positive
integers; zero, negatives, fractional numbers and overflow are invalid. Use the current one-year
age and one-million-failure bounds as initial limits. Omission is the sole way to disable a
threshold. Test equality at the boundary and the OR behavior when both thresholds are configured.

Expiry uses `now >= capturedAt + maxAgeSeconds`, with checked arithmetic. Capture obtains time from
the source database; consumer comparisons use its database time. Broker publication does not reset
age. Operations require reasonably synchronized database clocks; this is not a claim of perfect
wall-clock agreement. Expiry is checked before starting an attempt and after a confirmed failure;
it does not undo effects or cancel an already-running successful transaction solely due to age.

## Failure-count contract

| Stage | Count toward `maxFailures` | Do not count |
| --- | --- | --- |
| Outbox | Completed calls to publish an original event that fail to return a valid expected-stream ACK, including transport failure | Database failure/ambiguous commit, worker cancellation, retries of a discard notice, process crashes with no committed count |
| Consumer | Deliberate `HANDLER_REJECTED` failures after confirmed rollback of handler work | Broker redelivery count, receipt failures, parser/schema/identity conflicts, SQL failures, timeouts, cancellation, unknown exceptions or ambiguous commits |

Consumer handlers opt into a deterministic rejection using the existing rejection exception or a
narrow replacement. Other failures retry; they may still become eligible for an explicitly
configured age discard after database state is reconciled. Protecting database failures from the
failure counter does not pause the age clock. Only a successful audit transaction can discard.

## Notice and identity contract

A notice refers to the original source/ID and SHA-256 of its serialized bytes. It copies the
original delivery metadata and includes a bounded reason code, decision time and committed failure
count. Its own source remains the realm source, but its UUID, CloudEvent type and schema are
distinct. Retries of either representation must preserve its own UUID, subject and bytes.

Allow only producer `EXPIRED` or `FAILURE_LIMIT` notices authorized by the original outbox policy.
Validate the age/count evidence against that policy. Consumer-local discard does not publish a
global notice and cannot affect another application's cursor.

Consumer business filters select original subjects. A notice is control data, never a business
handler input, and must bypass those filters. Consumers reject conflicting references instead of
choosing arbitrary bytes for an identity or sequence.

## Acceptance and validation

- Parser and JSON schema agree on accepted/rejected examples; unknown fields never silently alter
  a retain policy into discard.
- Overlapping rules prove first-match behavior; scope, wildcard, custom resource, missing optional
  identity and `userEnabled` cases have focused coverage.
- Concurrent reload tests demonstrate one capture uses one snapshot. Failed reloads retain both
  capture and delivery behavior.
- Schema fixtures cover original events, notices, large valid sequence strings, overflow, invalid
  sequence encodings and original/notice reference mismatch.
- Existing filter files remain valid and resolve both stages to retry.
- Run `python scripts/validate.py`. Inspect packaged dependencies so the shared module does not
  accidentally package Keycloak APIs or duplicate a different Jackson version.

## Handoff

Provide the schema/type locations, example policy, policy-resolution API, notice fixtures and
validation results to phases 2–5. Production event emission remains phase 2's responsibility;
isolated contract fixtures must not be represented as a working ordering guarantee.
