# Phase 4: local expiry and discard

Prerequisites: [policy](01-contracts-and-policy.md), [capture](02-transactional-capture.md) and
[relay](03-ordered-relay-and-discard.md). Outcome: configured events can leave the outbox without
being published, with a local diagnostic record and no replacement message.

## Read first

- `OutboxEvent`, `OutboxRepository`, `OutboxRelay`, `RetryBackoff` and the resolved policy types.
- `extension/src/main/resources/META-INF/nats-outbox-changelog.xml`
- Existing failure/transaction tests and the new relay ownership tests.

## Work

1. Evaluate frozen policy before an original publication and after a counted failure. Reaching
   either configured threshold permits discard. If both are reached, use expiry as the diagnostic
   reason. Defaults continue retrying indefinitely.
2. Under row ownership, insert metadata-only audit and delete the pending original in one database
   transaction. If either write or commit fails, preserve the unresolved obligation. A successful
   commit makes the next position eligible; there is no NATS call in the discard path.
3. Add bounded expiry scanning independent of `nextAttemptAt`. Expiry may remove a queued successor
   while an earlier event remains blocked, but it must not publish another successor past that
   earlier unresolved row. Skip active locked publications rather than racing their outcome.
4. Remove original payload bytes when discard commits. Do not retain a replayable copy or create
   a synthetic Keycloak event. A successful publish requires no per-event diagnostic history.
5. Expose committed discard outcomes to metrics. A failed local audit transaction must never be
   reported as a completed discard or release a head.

## Audit contents and uncertainty

Use a separate extension table in Keycloak PostgreSQL. Record original event ID, realm, affected
ordering key/sequence when present, original event type/subject, payload hash, capture/discard times,
policy digest/rule, limits, committed failure count and a bounded reason code. Exclude payloads,
representations, arbitrary headers and raw exception text. The audit's purpose is local diagnosis.

Also record whether publication may have occurred. A timeout, interrupted network attempt or
uncertain database commit does not prove that NATS rejected the event. Even a zero committed
failure count may follow a crashed worker that sent bytes before its transaction rolled back.

For a reliable distinction, persist a sticky `publicationMayHaveOccurred` intent before the first
network send. Separate that short commit from the send transaction, then reacquire/recheck the
same head before publishing. Once set, never clear the flag on failure or retry. A crash between
intent and send can conservatively mark an unsent event ambiguous; it cannot mark a sent event
as definitely unpublished. Count failures only when they are durably recorded; the flag is not
itself an attempt count. Competing workers still cannot pass a pending predecessor.

If discard becomes eligible after intent but before send, deleting the row prevents later workers
from sending it; already-active network requests remain uncertain. Audit wording must describe
abandoned publication attempts, not guarantee that no broker copy exists. Never try to retract
accepted messages from the shared NATS stream.

## Acceptance and validation

- Protected events survive old age and many failures; explicitly discardable events reach the
  correct boundary, including OR semantics and equality.
- Discard works while NATS is unavailable and emits zero messages, including zero control/audit
  messages. Only later eligible Keycloak-derived events are published after recovery.
- Discard plus audit is atomic; audit rejection, rollback and uncertain commit never skip work.
- Discarding sequence 11 permits a later 12 without a gap-filling message. Existing original
  subjects and IDs remain unchanged.
- Non-head expiry removes only eligible payloads and does not bypass a retained predecessor.
- Crash after send intent, lost PubAck and late publisher tests preserve the ambiguous-outcome
  flag. A counter rollback cannot turn it into a definite non-delivery claim.
- No reader application, database or subscriber acknowledgement is needed to finalize discard.

```text
python scripts/validate.py
python scripts/validate.py integration --maven-arg=-Dit.test=OutboxDiscardIT,PerUserRelayIT
```

## Handoff

Provide atomic discard logic, intent/ownership boundaries, audit schema and failure evidence.
Phase 5 starts retention at the committed discard timestamp. No notice acknowledgement or
receiving-application state is part of that lifecycle.
