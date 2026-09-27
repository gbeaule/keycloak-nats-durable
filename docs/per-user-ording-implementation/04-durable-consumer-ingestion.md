# Phase 4: durable consumer ingestion

Prerequisite: [phase 1](01-contracts-and-policy.md); use
[phase 3](03-ordered-relay-and-discard.md) fixtures for integration. Outcome: consumers ACK complete
durable receipt independently of business processing and retain enough information to order work.

## Read first

- `consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/ConsumerMain.java`
- `ConsumerWorker.java`, `InboxProcessor.java`, `BoundedTransaction.java`, `ConsumerDatabase.java`
  in the same package.
- `consumer-example/src/main/resources/db/consumer.sql`
- Consumer configuration, inbox, bounded-transaction and hardening tests.
- Phase 1's shared original/notice parsers and identity rules.

## Change in acknowledgement ownership

The current example applies an effect and then ACKs. Replace that single stage with:

1. Receive and durably store the original message or notice, including its immutable identity/hash
   and metadata needed to process it after restart.
2. Commit receipt; ACK only after the commit is known to have succeeded, or a later read confirms
   the identical receipt already exists.
3. Independently schedule business processing in phase 5.

A broker ACK now means the consumer database owns the work, not that the business effect finished.
Do not mark an inbox entry applied merely because it was received. WorkQueue can remove the broker
copy at this point, so the persisted inbox must be a sufficient recovery copy.

## Work

1. Rework the fresh consumer schema for immutable receipts, per-key progress and terminal outcomes.
   Add the creation DDL directly; old consumer database migration is out of scope. Keep initialization
   idempotent for a database already created with the new schema.
2. Split `ConsumerWorker` into receipt/ACK responsibilities and the later processing scheduler.
   Receipt must never call an application handler, wait for a missing sequence or depend on that
   user's processing lock.
3. Parse originals and notices using the shared contract. Persist original bytes, subject, source/ID,
   hash, first receipt broker metadata, parsed ordering identity and frozen policy. For notices,
   also persist their original-event reference. Validate subject/envelope/reference consistency.
4. Deduplicate retransmissions by logical consumer plus source/transport ID, checking bytes/hash and
   subject. A republished ID can have a different broker sequence. Originals and notices have
   distinct transport IDs and must be allowed to coexist for one original sequence.
5. Enforce that one ordering slot cannot refer to two different originals. An original's ID/hash
   must match a notice's reference when both exist. Conflicts must not advance progress, silently
   replace payloads, or be treated as ordinary duplicates.
6. Validate the actual durable consumer as a complete-feed durable pull consumer with explicit ACKs,
   unlimited redelivery, disk state and `DeliverAll`. For the first implementation, require the
   configured prefix's entire `prefix.>` feed and reject narrower/multiple event-type filters,
   start-time/sequence policies, headers-only mode and incompatible inactivity settings.
7. Keep a bounded pending window and allow concurrent receipt across replicas. Do not set global
   `MaxAckPending=1` to implement business ordering. Receipt can arrive out of sequence; phase 5
   buffers until the missing sequence is present.
8. Provide bounded ingestion concurrency and independent processing connection capacity. A stalled
   handler must not consume the only connection needed for receipt. Backpressure pauses pulls
   without evicting pending inbox records.

## Persistence responsibilities

The implementation can select concrete table names, but must encode these distinct concepts:

| Record | Required meaning |
| --- | --- |
| Receipt | Durable transport identity, byte hash, original bytes while needed, broker reference and parsed routing/ordering/policy metadata. |
| Ordering progress | Logical consumer and key, last terminal sequence, current head retry/failure state; starts at 0. |
| Terminal outcome | Unique logical consumer/key/sequence; original identity/hash and applied, ignored or discarded disposition. |
| Discard audit | Metadata proving the local discard or producer notice decision; may be part of the terminal-outcome table. |
| Invalid receipt | Durable raw input and bounded validation diagnostics when a trusted ordering identity/policy cannot be established. |

Use database uniqueness constraints, not in-memory maps, for idempotency and slot identity.
Append-only receipt insertion must not lock progress rows held by business handlers. If slot
reference validation requires a separate registry, its short receipt locks must remain independent
of the processing lock. Demonstrate that a same-user arrival does not freeze all ingress behind a
running handler.

Repeated delivery after terminal payload cleanup must validate against retained hashes and terminal
identity. It must not repopulate a discarded payload. Preserve the identity/reference evidence needed
to accept a valid late notice or reject a conflicting late original.

Malformed input has no trusted permission to discard itself. Store it durably as unresolved invalid
input before ACK, without executing it or advancing any claimed sequence. If durable storage cannot
commit, leave it unacknowledged. Oversize/invalid-input handling must remain bounded: do not ACK an
input whose complete recovery bytes cannot be stored under configured limits. Invalid data can
leave a real user's sequence gap; report it for repair rather than skipping that gap. This retained
unresolved input is not a terminal replayable quarantine action.

## Complete-history requirement

The scheduler's initial cursor is zero; it expects sequence 1. A new logical consumer needs complete
retained history for its feed. Reusing the same logical consumer requires its existing inbox and
cursors. Do not initialize a cursor to `firstSeenSequence - 1`: a low sequence may merely be delayed.

Changing broker filters or attaching a fresh database to an already-advanced durable is unsupported
without an explicit recovery procedure. Validate the configuration that can be checked at startup;
do not claim configuration validation can prove all historical data still exists. Persistent gaps
must be visible in monitoring.

## Acceptance and validation

- Crash after receipt commit but before ACK: redelivery confirms one stored receipt, and later
  processing can still apply the effect.
- Crash after ACK but before processing: restart uses only the inbox to complete work.
- Receipt rollback, ambiguous commit and identity conflict never produce an unsafe ACK.
- Different transport IDs for an original and notice coexist; mismatched references are detected
   in either arrival order and prevent processing of an unresolved disputed slot. If it is already
   terminal, preserve that result and report the conflict; do not attempt to undo committed effects.
- A forged or malformed policy cannot authorize discard. Invalid messages can be retained without
  blocking receipt of valid messages when resources remain available.
- Sequence 3 can be received before 1 and 2, with no premature cursor initialization or effect.
- Multiple receipt replicas deduplicate both broker redelivery and republishing outside the broker
  deduplication window.
- Actual durable configuration validation rejects incomplete feeds. Business selection does not
  filter notices out at the broker.
- Inbox capacity/pool limits produce backpressure, not payload deletion or receipt ACK before commit.
- Run quick checks and real broker/database tests:

```text
python scripts/validate.py
python scripts/validate.py integration --maven-arg=-Dit.test=OrderedInboxIT,ConsumerHardeningIT
```

## Handoff

Provide receipt APIs, schema/uniqueness constraints, duplicate/conflict handling, connection budgets
and crash-test evidence. Phase 5 must build on the durable receipt state without restoring the old
assumption that an inbox identity already means an applied effect.
