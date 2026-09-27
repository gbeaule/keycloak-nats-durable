# Phase 2: transactional per-user capture

Prerequisite: [phase 1](01-contracts-and-policy.md). Outcome: every accepted event has a durable
ordering key, consecutive sequence and frozen delivery policy in the same transaction as capture.

## Read first

- `extension/src/main/java/io/github/gbeaule/keycloaknats/DurableEventListener.java`
- `extension/src/main/java/io/github/gbeaule/keycloaknats/EventEnvelope.java`
- `extension/src/main/java/io/github/gbeaule/keycloaknats/OutboxEvent.java`
- `extension/src/main/java/io/github/gbeaule/keycloaknats/OutboxEntityProviderFactory.java`
- `extension/src/main/resources/META-INF/nats-outbox-changelog.xml`
- `DurableEventListenerTest`, `EventEnvelopeTest`, `OutboxChangelogTest` in extension tests.
- `DurabilityIT`, `CustomSchemaIT`, `UpgradeIT` and `FilteringIT` in integration tests.

## Work

1. Separate event description/identity resolution from final serialization. Resolve one immutable
   filter snapshot, apply capture selection, and allocate no sequence for an excluded event.
2. Introduce a capture-counter entity keyed by realm and affected user. Atomically create/increment
   it in the active Keycloak transaction. The row lock is retained through commit or rollback.
   Use a transactional row update/upsert, not `nextval`, UUID order, timestamps or `MAX + 1` over
   pending outbox rows.
3. Obtain database capture time and resolved delivery metadata, then serialize the final event
   once. Persist event, sequence/counter update and policy together; flush failures must continue
   marking the Keycloak transaction rollback-only.
4. Extend the outbox row with queryable ordering scope/key/sequence, capture time, resolved policy
   fields, policy/rule identities and publication state. Add indexes and uniqueness constraints
   needed by phase 3. Keep original immutable wire bytes separate from mutable retry/state fields.
5. Rework the initial Liquibase schema and entity registration for fresh installations. Add the
   discard audit and notice storage planned for phase 3, or coordinate that exact shape with its
   implementation. Remove tests that assert obsolete pre-release checksums; replace them with
   fresh-schema structure and behavior checks.
6. Update serialization examples, schema tests and fresh fixtures. Preserve synchronous-commit and
   after-commit wakeup behavior already protected by integration tests.

## Affected-user resolution

Use one resolver for business metadata, sequence keys and policy context. Keep a separate
direct-user predicate for `userEnabled` lookup and matching so nested resources never trigger the
direct-user state observation accidentally.

| Source | Attribution |
| --- | --- |
| User event with a nonblank `userId` | That user in the event's realm. |
| Direct USER admin event at `users/{id}` | The path's target user. |
| Recognized nested user admin event | The user segment of a tested Keycloak user-resource path. |
| Missing, ambiguous or unrelated identity | Singleton event key with sequence 1. |

Inspect supported Keycloak resources and actual emitted paths before defining the nested-path
allowlist. Cover role mappings, group membership, credentials and consent where the supported
runtime emits those forms. Pair path structure with compatible resource types; do not treat any
custom resource string containing `users/` as proof. Reject empty/malformed segments and avoid
lossy or repeated URL decoding. Test storage-backed user IDs rather than assuming every ID is a UUID.
An event may have an error and still have a valid affected-user identity.

Do not look up a deleted user to obtain its ordering key. The realm comes from the affected event,
not admin authentication details. Populate nested admin `data.userId` with the recognized affected
user; preserve `actorUserId` separately. Unrecognized/custom forms remain singleton events until
their attribution has explicit tests.

## Database invariants

- Sequence starts at 1 and increments once per accepted event. A rolled-back transaction leaves
  neither its events nor consumed sequence values. Several events in one transaction are ordered
  by callback order.
- Counter state survives an empty outbox and user deletion. Do not use a cascading foreign key to
  Keycloak's user row, and do not reset counters when the relay drains.
- A higher sequence cannot commit while a transaction holding a lower sequence remains open.
- Realm/user tuple uniqueness and `(orderingKey, sequence)` outbox uniqueness are database-enforced.
  Check identifier lengths and sequence overflow explicitly; never truncate or wrap.
- Capture and publication ownership use separate rows. A relay may not lock/update the capture
  counter during a network call.
- Use Keycloak's managed connection and transaction. Native SQL, if needed, must respect custom
  schemas and identifier quoting; do not create a second datasource.
- Establish a consistent lock order where multiple users are known in advance. Listener callbacks
  may discover users incrementally; deadlock or lock-timeout victims must roll back the entire
  Keycloak transaction, not silently omit an event or retry a fragment of account work.

No migration means fresh test databases use the new initial schema. It does not mean dropping
Keycloak-version compatibility checks. Adapt `UpgradeIT` to test the same new provider/schema
across supported Keycloak runtime transitions where applicable, without loading the old feature
schema or adding a legacy converter.

## Acceptance and validation

- Real PostgreSQL transactions demonstrate same-user contention, reversed attempted commit order,
  rollback without a gap, simultaneous first-user insertion and multiple callbacks in one transaction.
- Different users and identical user IDs in different realms allocate independently.
- Userless events do not contend on a shared counter. Deletion and an empty outbox do not reset a
  known user's sequence.
- Nested attribution tests include valid forms and plausible false positives, including the admin
  actor being different from the target.
- Policy reload during a held transaction does not change captured payload/policy. Excluded events
  do not leave counters or sequence gaps.
- Fresh installation works in a non-default database schema. Capture failure still rolls back the
  associated Keycloak mutation, and authentication-only transactions retain durable commits.
- Run `python scripts/validate.py` and focused container coverage, for example:

```text
python scripts/validate.py integration --maven-arg=-Dit.test=PerUserCaptureIT,CustomSchemaIT,FilteringIT
```

Create `PerUserCaptureIT` or use equally explicit existing tests; keep command selections in sync
with actual test names. Phase 7 adds the new lock tests to the full compatibility matrix.

## Handoff

Provide the counter and outbox entities, actual constraints/indexes, attribution rules and capture
test results. State the exact database lock order for phase 3. Ordering at capture alone does not
yet establish ordered publication or consumer effects.
