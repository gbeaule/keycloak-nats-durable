# Phase 5: independent per-user processing and discard

Prerequisites: [contracts](01-contracts-and-policy.md) and
[durable ingestion](04-durable-consumer-ingestion.md). Use the
[relay](03-ordered-relay-and-discard.md) for end-to-end verification. Outcome: independent consumer
workers apply or explicitly skip each user's events in sequence with durable failure accounting.

## Read first

- Phase 4's receipt, progress, identity and terminal-outcome persistence.
- `consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/InboxProcessor.java`
- `BoundedTransaction.java`, `RejectedEventException.java`, `FailurePolicy.java` and
  `ProcessingLimits.java` in the same package.
- Existing consumer transaction, deadline, idempotency and recovery integration tests.

## Scheduling algorithm

For a logical consumer, discover keys with received work and create any missing progress row with
last terminal sequence 0. Discovery must not infer that a missing lower sequence was filtered or
discarded. Keep the discovery path separate from receipt transactions.

1. Claim an eligible key's progress row using a database lock and `SKIP LOCKED` across keys. A due
   retry applies only to its current head. An available valid producer notice or a reached age
   threshold must be considered even if an original retry is not yet due.
2. Read only `lastTerminalSequence + 1`. A later receipt does not make this key ready. Release it
   when the next slot has no original, valid notice or durable local discard decision.
3. Validate all representations and reference identities for the slot. Select its outcome using
   the rules below. One worker holds the per-key lock throughout the transaction, so replicas
   cannot simultaneously advance the same user.
4. Apply the handler, or record the permitted discard/ignore disposition, and advance the cursor
   in one commit. Commit one terminal sequence per scheduling unit, then yield/requeue the key
   fairly so a busy user cannot monopolize workers.
5. On retry, retain the head and its payload, persist eligible failure/backoff state, and release
   the key. Other users remain claimable.

Use bounded worker concurrency with a configurable processing pool and enough database capacity
for independent receipt. A failing user's retry must not consume all scheduling opportunities.
Singleton event keys follow the same algorithm with one sequence; they do not share a global lock.

## Outcome selection

| Available state | Result |
| --- | --- |
| Matching terminal outcome already exists | Validate identity and do not execute again. |
| Valid producer discard notice, with no committed terminal result | Discard this sequence without running its handler. |
| Durable consumer-local discard decision | Finish that discard when the sequence becomes the head. |
| Original excluded by local business subject selection | Record `IGNORED` and advance; notices are never subject-filtered. |
| Original's consumer age/failure limit permits discard | Commit metadata-only discard and advance. |
| Eligible original with no permitted discard | Execute handler within the bounded transaction. |
| Missing predecessor, conflict or untrusted representation | Retain and report; do not advance. |

The first committed terminal disposition is final. A notice received after application records
that the notice was observed after the effect; it does not reverse the effect or imply that the
producer's discard decision happened after application.
A late original after a discard is validated and suppressed without reconstructing its payload.
If a handler is already running when a notice arrives, the per-key terminal transaction determines
the winner. Do not attempt to cancel an already-committing effect to force a preferred race outcome.
If both representations are available before selecting work, prefer the valid notice.

A producer notice uses the source event's outbox authorization, even when that event's consumer
stage says retry. The producer abandoned that original globally. A consumer-local discard uses
only the consumer policy and affects only its own logical application's sequence.

## Atomic effects and failed-attempt accounting

Acquire the per-user lock before a handler savepoint. Execute handler database effects after that
savepoint. On a deliberate `HANDLER_REJECTED` failure, roll back handler work to the savepoint
before recording a counted failure, a retry time, or a failure-limit discard. These updates commit
in the outer transaction while the per-user lock is still held.

Do not increment from NATS delivery counts. Receipt ACK loss can cause many deliveries without
another business attempt. Unknown exceptions, SQL failures, timeouts, cancellation or rollback
failure must roll back the outer transaction and remain uncounted infrastructure/unknown failures.
If the existing transaction helper cannot safely support this boundary, refactor it with tests;
do not persist a failure in an unrelated transaction after releasing ownership without a durable
attempt identity that prevents races and double counting.

Preserve deadline cancellation and the guard against a timed-out handler committing later. If a
database commit result is ambiguous, a subsequent worker resolves the persisted cursor/outcome
under the same lock. It must not declare a discard or rerun effects merely because the first
worker could not observe the commit.

Handler effects, outcome identity and cursor advance share one commit. Savepoint rollback must
remove all rejected handler writes. Handlers must use the supplied connection and must not commit,
alter transaction ownership, or perform unprotected external effects.

## Expiry behind a blocked user

Provide bounded expiry scanning for received originals, including ones behind a missing or retained
predecessor. A permitted non-head expiry may commit an audit/decision and clear its business payload,
but cannot advance the user's cursor or mark an out-of-sequence terminal completion. Keep the
original identity/hash, delivery policy and pending discard decision until its turn arrives.

The expiry scanner must exclude active processing through compatible row/slot ownership, using
bounded locks or `SKIP LOCKED`. Specify and test lock order between user progress and slot state.
It must neither erase data from a running retained attempt nor block ingestion on the handler's
long-lived progress lock. Age scans operate on resolved original policies, never on notices or
untrusted invalid receipts.

## Local filtering and cleanup

Add an optional local subject selection, default all, evaluated on original business subjects after
receipt. Persist the selected/ignored decision at receipt so a process restart or changed local
configuration cannot reinterpret an already-received event. Existing receipts keep that decision;
broker redelivery never recalculates it. All replicas of a logical consumer must use the same local
selection configuration.

Retain terminal identities, hashes and cursors for the possible replay horizon. Terminal payloads
may be removed transactionally; discard audits never contain replayable payloads. On a late
duplicate, compare retained evidence and avoid restoring cleared bytes. Do not add automatic
deduplication/cursor/audit expiry in this feature.

The old terminal quarantine/replay workflow is incompatible with metadata-only discard and must
not be used as an alternate path around sequencing. Phase 6 removes its runtime commands/settings;
this phase replaces processing references to it. Pending inbox storage is still a durable recovery
copy for unresolved work.

## Acceptance and validation

- Two replicas cannot execute two handlers for the same user concurrently. A blocked A1 prevents
  A2 while B1 and B2 finish in order.
- Receive A3 before A1/A2 and prove no premature effect. A valid notice for A2 allows A3 after A1;
  an absent notice does not.
- Test original then notice, notice then original, duplicates of both, and notices racing a running
  handler. One terminal outcome and at most one committed effect remain.
- A policy-protected event never discards because of high delivery counts or another event's rule.
- Eligible handler failures roll back their writes, increment exactly once and discard at the
  configured count. Retry backoff, failure count and cursor survive restart.
- Age can cause discard without any failure and includes time spent in the source outbox. Expiry
  behind a gap removes payload without falsely completing later sequences.
- Receipt continues while a slow handler holds the user's processing lock. A hot user's backlog
  does not starve unrelated users when worker/database capacity is available.
- Crash or ambiguous commit before/after effect completion cannot produce an out-of-order or
  repeated effect. Deadline/unresponsive-handler guarantees remain intact.
- Local filtering records ordered `IGNORED` outcomes, not gaps; notices bypass selection.
- Run quick checks and focused real database/broker tests:

```text
python scripts/validate.py
python scripts/validate.py integration --maven-arg=-Dit.test=PerUserConsumerIT,OrderedInboxIT,ConsumerHardeningIT
```

## Handoff

Provide the actual lock order, transaction/savepoint boundaries, failure classifications, scheduler
settings and race-test results. Give phase 6 the distinction between receipt, pending decisions and
terminal outcomes so metrics and operational commands do not conflate them.
