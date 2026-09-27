# Phase 3: per-user relay ordering

Prerequisite: [transactional capture](02-transactional-capture.md). Outcome: workers coordinate
publication ownership by user without waiting on any downstream application.

## Read first

- `extension/src/main/java/io/github/gbeaule/keycloaknats/OutboxRepository.java`
- `OutboxRelay.java`, `RelayWorker.java`, `DurableEventListenerFactory.java`,
  `JetStreamPublisher.java`, `RetryBackoff.java` in the same package.
- `nats-transport/src/main/java/io/github/gbeaule/keycloaknats/jetstream/StreamPolicy.java`
- Existing relay/publisher tests and `DurabilityIT`, `NatsClusterIT`, `CustomSchemaIT`.

## Work

1. Claim a due outbox row only if no lower unresolved sequence exists for its user. The predecessor
   check includes locked rows and rows waiting for retry. Apply `SKIP LOCKED` to eligible heads;
   skipping a locked head must never make its successor eligible. Userless rows are independent.
2. Hold publication-row ownership through the bounded publish request and database resolution.
   Never hold the capture-counter lock over a broker operation. Revalidate state/version after
   ownership acquisition so stale workers cannot delete a changed row.
3. On a valid expected-stream PubAck, remove the original in the same database transaction. On
   failure, persist the eligible failure count and retry time. Database rollback keeps the row.
   Preserve exact original bytes, subject and message ID for every retry.
4. Return explicit batch outcomes such as published, retry scheduled, no work and stopped. Phase 4
   adds discarded. A failing user's head must not terminate useful work for other users. Count
   all work against bounded batch limits and avoid spinning on ineligible heads.
5. Keep existing stream-safety checks and bounded timeouts. Do not subscribe to business subjects,
   create consumer state or use delivery ACKs to determine publication completion.
6. Preserve enough durable attempt information for phase 4 to distinguish never-attempted events
   from events that may have reached NATS. A transaction rollback cannot erase the fact that a
   network request might already have been sent; use the conservative ambiguous classification
   described in phase 4 rather than interpreting a rolled-back counter as proof of no send.

## Contract and failure boundaries

A later position is not intentionally started until predecessors have confirmed publication and
committed removal, or phase 4 has committed their local discard. This describes publisher work
ordering. It does not promise chronological subscriber processing or duplicate-free arrival.

A valid PubAck followed by database commit failure requires retrying the original ID. A worker
whose database transaction died may still have an in-flight publish. State checks prevent stale
local mutations; they cannot withdraw already-sent bytes. Retrying after a deduplication window can
produce another broker copy. Do not hide these cases in a strict receive-order assertion.

## Acceptance and validation

- Two workers/nodes cannot claim successive unresolved events for one user simultaneously.
- A locked or not-yet-due A1 blocks A2, while B1 proceeds. Different event subjects share A's order.
- Rollback and uncommitted capture cannot expose later positions prematurely.
- Lost PubAck and failed database removal retain original retry identity and payload.
- A stale worker cannot delete a newer row state after losing ownership.
- Failing heads neither starve unrelated users nor cause unbounded busy loops.
- Actual PostgreSQL query/index behavior works in custom schemas. Use real locks and broker tests,
  not only mocked repository results.

```text
python scripts/validate.py
python scripts/validate.py integration --maven-arg=-Dit.test=PerUserRelayIT,DurabilityIT,NatsClusterIT
```

Create the named focused suite or keep these selections aligned with actual tests.

## Handoff

Provide the head query, indexes, lock order, batch outcomes and fault-test results. Phase 4 must
release a discarded position through the same database ownership rules, without publishing a
replacement message. State any observed broker-arrival limitations explicitly.
