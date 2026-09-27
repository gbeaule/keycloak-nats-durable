# Phase 1: publication policy and event contract

Prerequisite: [feature boundary and decisions](README.md). Outcome: deterministic capture and
publication-policy selection, plus ordering metadata on Keycloak-derived events.

## Read first

- `extension/src/main/java/io/github/gbeaule/keycloaknats/EventFilter.java`
- `CaptureScope.java`, `ReloadingEventFilter.java`, `EventEnvelope.java` in that package.
- `schemas/event-v1.schema.json`, the Keycloak catalogue and `config/events-*.json`.
- Existing filter, reload, envelope and schema tests.

## Work

1. Add an optional `delivery.rules` section and a JSON schema for the existing filter document.
   Capture selection runs first. A delivery rule cannot capture an excluded event.
2. Compile capture and delivery rules into one immutable snapshot. Each callback reads one snapshot,
   including admin preselection and observed enabled-state matching. Invalid initial files fail
   startup; invalid reloads retain the entire last valid snapshot.
3. Resolve one internal publication policy per accepted event, with the filter-byte SHA-256 and a
   rule ID. Persist those with the outbox in phase 2. Retrying never reinterprets current config.
4. Add `data.ordering` to the unreleased event schema for confidently attributable user events:
   an unambiguous `key` and a positive decimal-string `sequence`, backed by checked 64-bit storage.
   Use `u.<base64url realm ID>.<base64url user ID>` for the key. Omit ordering for userless events.
5. Preserve business subjects, CloudEvent identity and the existing event schema identifier. Update
   JSON examples and validators. No discard schema, control subject or synthetic event type exists.
6. Keep policy types in the extension. Do not add a shared consumer-policy module. Rule IDs,
   failure limits, audit details and delivery decisions stay internal unless already needed by the
   business-event contract; no receiver implements the publication policy.

## Rule semantics

Rules have unique nonblank IDs, a `match` object and required `policy`. First matching rule wins;
no merge or inferred specificity. Omitted delivery rules or no match means retry indefinitely.
User matches require `kind: user` and `eventTypes`; admin matches require `kind: admin`,
`resourceType` and `operations`. Reuse current scope, wildcard and custom-resource semantics.
`userEnabled` remains restricted to successful direct USER CREATE/UPDATE observations.

```json
{
  "userEvents": ["*"],
  "adminEvents": [{ "resourceType": "*", "operations": ["*"] }],
  "delivery": {
    "rules": [
      {
        "id": "short-lived-login",
        "match": { "kind": "user", "eventTypes": ["LOGIN", "LOGIN_ERROR"] },
        "policy": {
          "action": "discard",
          "maxAgeSeconds": 300,
          "maxFailures": 20
        }
      }
    ]
  }
}
```

These thresholds are illustrative. Unmatched events retain the no-discard default. `retry` forbids
thresholds; `discard` requires at least one. Positive integer age/failure limits combine with OR.
Use one year and one million failures as initial validation bounds. Reject zero, overflow,
unknown/duplicate fields, invalid selectors and contradictory combinations. Audit retention is a
separate maintenance setting and may allow zero; do not confuse it with event expiry.

Expiry is `databaseNow >= capturedAt + maxAgeSeconds` with checked arithmetic. Count completed
original publication calls that fail to return a valid expected-stream ACK, including transport
failures. Do not count cancellation, database failures, uncertain database commits or hypothetical
attempts lost in a crash. Receiving-application failures are unrelated.

## Acceptance and validation

- Schema and parser agree on accepted/rejected fixtures, rule precedence and safe defaults.
- Existing filter files remain valid and never introduce implicit discard.
- Concurrent reloads cannot mix capture rules from one snapshot with policy from another.
- Sequence encodings reject overflow and malformed values; events without a user omit ordering.
- All emitted message shapes remain derived from actual captured Keycloak events.
- Run `python scripts/validate.py`. Phase 2 wires the new metadata to real capture transactions.

## Handoff

Provide parser/schema locations, resolved-policy API, ordering fixtures and validation results.
No consumer API, mandatory subscriber filter or receiver processing feature is part of this phase.
