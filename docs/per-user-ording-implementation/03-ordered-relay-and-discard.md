# Phase 3: ordered relay, expiry and discard notices

Prerequisites: [contracts](01-contracts-and-policy.md) and
[capture](02-transactional-capture.md). Outcome: relay workers resolve each user's publication
obligations in sequence, with durable and auditable policy-authorized discard.

## Read first

- `extension/src/main/java/io/github/gbeaule/keycloaknats/OutboxRepository.java`
- `extension/src/main/java/io/github/gbeaule/keycloaknats/OutboxRelay.java`
- `extension/src/main/java/io/github/gbeaule/keycloaknats/RelayWorker.java`
- `extension/src/main/java/io/github/gbeaule/keycloaknats/DurableEventListenerFactory.java`
- `extension/src/main/java/io/github/gbeaule/keycloaknats/JetStreamPublisher.java`
- `extension/src/main/java/io/github/gbeaule/keycloaknats/RetryBackoff.java`
- `nats-transport/src/main/java/io/github/gbeaule/keycloaknats/jetstream/StreamPolicy.java`
- Existing relay, publisher, durability and NATS-cluster tests.

## Publication state machine

| State | Allowed transition | Required durable evidence |
| --- | --- | --- |
| `EVENT_PENDING` | Publish original and remove resolved outbox row | Expected stream's valid PubAck, followed by committed database removal |
| `EVENT_PENDING` | Schedule original retry | Persisted failure count/backoff when the failure transaction commits |
| `EVENT_PENDING` | `NOTICE_PENDING` | Policy threshold reached; audit and immutable notice committed atomically with clearing original payload |
| `NOTICE_PENDING` | Publish notice and remove resolved outbox row | Notice PubAck and committed database removal/audit update |
| `NOTICE_PENDING` | Schedule notice retry | Retained notice identity and bytes; no notice expiry or failure-limit discard |

The capture counter survives all of these transitions. A failed database commit resolves nothing.
Publication outcomes remain uncertain across database/network failure; retry original or notice
with its own persisted identity according to the currently committed state.

## Work

1. Replace next-due-row claiming with next-due-head claiming. A candidate is eligible only if no
   lower unresolved sequence exists for its ordering key. The predecessor check includes rows
   that are locked, not yet due, or waiting to publish a notice. Apply `SKIP LOCKED` only to eligible
   head rows; skipping a locked head must never make its successor eligible.
2. Keep one head's row lock through its bounded publication and database resolution. Recheck state
   after obtaining ownership. Conditional state/version checks must prevent a stale worker from
   deleting or rewriting an obligation that changed after its transaction lost ownership.
3. Enforce the frozen outbox policy before starting an original publication and after a counted
   failure. Expiry wins as the reason when both limits are reached at the decision time. Use the
   database clock, not the event's Keycloak timestamp or a newly loaded filter policy.
4. Implement a bounded expiry sweep independent of normal retry eligibility. It may convert an
   expired non-head original to a pending notice, releasing its business payload even while an
   earlier sequence is blocked. It must not publish that notice or advance past predecessors.
5. Commit discard audit, stable notice UUID/bytes and `NOTICE_PENDING` state before attempting to
   publish the notice in a subsequent transaction. Clear the original payload as part of the
   committed transition; retain original identity/hash and policy evidence. A failed audit write
   or commit must preserve the original obligation and payload.
6. Publish notices through the same authenticated publisher and stream checks, using their own
   subject and message ID. Account for maximum notice size and header overhead in validation.
7. Replace the current boolean batch result with explicit outcomes such as published, notice
   prepared, retry scheduled, no work and stopped. A failed user must not end all useful work in a
   batch. Count every attempted unit against a bounded work budget; avoid hot loops on failed keys.
8. Add metrics hooks for retries, payload discard, notice backlog and blocked users. Phase 6 owns
   exporting them, but state transitions must expose reliable committed outcomes now.

## Discard audit

Record original source/ID, realm and ordering identity, sequence, original subject and payload
hash, capture/decision times, policy digest/rule, resolved limits, committed failures and reason.
Track the notice ID and whether its publication obligation has resolved. Keep bounded diagnostic
codes rather than payloads, headers or raw remote exception text.

One original can have only one committed producer discard decision. Never regenerate the notice
UUID or bytes on retries, including retries after a failed database deletion. Audit retention is
separate from pending-work cleanup; do not automatically purge it in this phase.

The audit means the producer abandoned further original publication. It is not a global statement
that the original was never published or processed. A timeout can leave an accepted original in
JetStream, and another worker may already have retried it. The distinct notice ID prevents
deduplication from suppressing the notice as if it were the original message.

## Failure behavior

- Broker outages can count as original publish failures for explicitly discardable events. Retain
  events continue indefinitely. Database failure and cancellation never fabricate a failure count.
- A discard is allowed only after the policy decision and audit can commit. In particular, a
  timeout while committing a discard is resolved by rereading committed state before acting.
- A missing/bad stream remains subject to all existing safety checks, even for notices. Do not
  enable stream age eviction or per-message TTL to make expiry work.
- If an original was accepted but its ACK or deletion was lost, either another original retry or
  a later valid discard decision may follow. Consumers reconcile original and notice as one
  sequence in phase 5. Do not attempt to delete accepted originals from the shared stream.
- A database lock cannot cancel bytes already sent by a stale network client. Correctness must
  tolerate late original and notice arrivals; it cannot depend on perfect publisher fencing.

## Acceptance and validation

- With two workers/nodes, locking user A's head prevents publication of A's successor while user
  B progresses. A not-yet-due head has the same effect for its own user only.
- Original publication order follows captured sequence across different event subjects. Consumer
  sequence enforcement remains required for duplicates, stale publishers and network ambiguity.
- An age deadline earlier than the next retry is honored by the expiry sweep. An expired successor
  loses its payload safely but its notice cannot overtake a retained predecessor.
- Default retry events survive arbitrarily old age and many attempts in controlled fixtures.
- Boundary and OR-policy tests cover immediate threshold crossing and same-transaction rollback
  of a would-be discard decision.
- Crash/fault injection covers before audit commit, after notice persistence, after original or
  notice acceptance but before ACK, and after ACK but before database commit.
- Duplicate-window expiry does not change notice identity. Stream rejection retains notices.
- A bounded batch can retry/discard A and publish B; repeated failures do not spin indefinitely.
- Non-default database schemas, original immutable-retry tests and query-index behavior remain
  covered. Permit payload removal only in the explicit committed discard transition.
- Run quick checks and targeted real database/broker tests, for example:

```text
python scripts/validate.py
python scripts/validate.py integration --maven-arg=-Dit.test=PerUserRelayIT,DurabilityIT,NatsClusterIT
```

## Handoff

Provide the implemented state machine, head query, indexes, immutable notice fixtures and injected
failure results. Explain which audit outcomes are uncertain publication outcomes. Phase 4 needs
real original-plus-notice fixtures; phase 5 must test both arrival orders.
