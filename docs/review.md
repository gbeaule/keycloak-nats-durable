# Implementation review

## Changes from the first implementation

| Finding | Change |
|---|---|
| Formatting did not require braces or use a complete named standard | Google Java Format plus the full Google Checkstyle ruleset for production and tests, warning-level build failures, explicit imports and public API documentation; IntelliJ settings are in `.editorconfig` |
| Dense URI condition and silently ignored trailing comma | Named URI checks, explicit port range and rejection of empty server-list entries |
| Mixed `put` styles obscured optional-field behavior | Uniform `data.put` calls, with null omission in one documented serialization step |
| Empty NATS error listener and unhelpful stream-failure logs | Connection/error diagnostics, safe failure reasons, JetStream error codes, cause categories and retry timing |
| Magic `-2` and query mixed with delivery logic | The JPA repository uses Hibernate's `Timeouts.SKIP_LOCKED_MILLI` and `SpecHints.HINT_SPEC_LOCK_TIMEOUT` constants |
| Fixed polling on idle nodes | Commit wakeups, a bounded pending flag preventing lost wakeups, adaptive idle scans and immediate full-batch draining |
| Growing wakeup counter | Removed the counter; notifications coalesce into one boolean under the same monitor as waiting |
| Shutdown waited behind publisher network I/O | Separate publication and lifecycle monitors; close cancels requests and late connections are closed instead of installed |
| Shutdown during database lookup could start another publish | Relay checks stop/interrupt state again after obtaining the row |
| `userEnabled` could also appear for ACTION | Observations restricted to successful direct USER CREATE/UPDATE, matching the documented contract |
| Admin routing required inspecting every resource payload | Subject includes resource type; old narrow-filter migration documented |
| Sparse event contract and only one runtime target | Full enum catalogue, twelve payload examples, JSON Schema validation, runtime matrix, manual candidate runs and an existing-database upgrade test |
| CI trust assumptions were implicit | Pinned actions, no persisted checkout credentials, cancellation/time limits and repository-setting guidance |

## Race and failure review

| Interleaving | Protection / evidence |
|---|---|
| Capture fails while Keycloak catches listener exceptions | Explicit rollback-only plus flush; real account-update rollback test |
| Relay scans before the account transaction commits | Database isolation hides uncommitted rows; explicit uncommitted/rollback integration test |
| Keycloak optimizes authentication-only transactions using asynchronous database commit | The outbox does not opt into ephemeral storage; a deferred-trigger test checks the commit-time setting |
| Commit occurs after the relay scans but before it sleeps | Pending flag is consumed only by the wait; latch-controlled scan/wait regression test |
| Many commits arrive before the worker wakes | One coalesced signal; burst regression verifies the next wait actually sleeps |
| Node dies after commit but before notifying its relay | Periodic database scan; test inserts committed work with no local wakeup |
| Two nodes select the earliest due row | Row lock held through publish and delete/commit; two-node test and held-due-row progress test |
| Process dies after NATS acceptance but before outbox removal commits | Original ID/bytes survive, broker dedup within its window, consumer inbox afterward; hard-kill test |
| Consumer commits an effect but dies before ACK | Atomic inbox/effect transaction, confirmed ACK afterward; redelivery and concurrent inbox tests |
| Stream fills during outage recovery | DiscardNew returns failure, outbox row retained until space exists; integration test |
| Shutdown interrupts publish | Row remains recoverable and thread interrupt is retained; regression test |
| Shutdown occurs while obtaining a database row | A second stop/interrupt check prevents publication and retains the row |
| Shutdown occurs during initial connection establishment | Close returns without waiting for connect; the late connection is closed and never installed; latch-controlled test |
| Shutdown occurs during stream validation or publication | Close can reach the connection independently of publication; both blocking-request interleavings are tested |
| Two callers attempt the first publication together | Publication monitor serializes connection creation; concurrent test verifies one connection |
| Wakeup counter wraps, or attempt count reaches its limit | Wakeup has no counter; persisted attempts saturate and continue scheduling retries; numeric-boundary test |
| Keycloak is replaced while the broker is unavailable | Existing-database replacement test checks the exact persisted IDs, subjects and bytes, recovery delivery and new capture |
| Admin update/delete events are reordered | **No ordering guarantee**; downstream state projections must reconcile, not blindly apply arrival order |
| Operator changes stream retention between validation and publish | No atomic configuration-and-publish API; restrict stream administration. Validation detects existing unsafe settings but cannot prevent concurrent privileged changes |
| Nodes sharing an outbox use different destinations | Operational requirement: same NATS account/stream/prefix on every node; destination is not persisted per row |

## Accepted API decision and validation limits

The documented event-listener and custom JPA registration APIs are the accepted product architecture. They keep event capture in the same transaction and database as the account change. The [compatibility policy](compatibility.md) defines candidate-version testing and upgrade review. Upstream's unsupported/internal classification still means we must verify each version; it is no longer an unresolved product decision.

The identified implementation races have regression coverage. The publish/commit and effect/ACK gaps are handled with persisted identity and transactional deduplication; they cannot be removed by an in-process lock. Event ordering, privileged stream changes and inconsistent cluster destinations remain explicit protocol/deployment boundaries. This review does not claim a proof that no other race is possible.

The implementation requires PostgreSQL; it does not silently claim compatibility with every database Keycloak supports. The outbox shares Keycloak's database, schema configuration and pool. Consumers needing durable exactly-once database effects also need an inbox in their application's transactional database; that is a consumer requirement, not another Keycloak database service.

The expanded suite includes a three-broker NATS cluster with client and route mutual TLS, leader failure, quorum loss, durable ACK state, and two Keycloak nodes. Power-loss behavior, deployment-specific TLS/JWT credentials, capacity under sustained load, database failover, restoration of multiple stores and external/federated user storage still require deployment-specific validation. See [the verification record](testing.md) for which runs passed.

## Capture, transport and storage review

The [external webhook listener review](listener-comparison.md) confirms that avoiding custom JPA alone would weaken durability. Capture filtering now occurs before persistence; a complete immutable policy is swapped on valid reload, and invalid reloads retain the previous policy. Backlog delivery is independent of the current capture policy.

Optional TLS loads mounted PEM trust roots and paired client credentials through a shared transport module. Bouncy Castle handles private-key PEM parsing and conversion; the JDK handles certificates, key managers and TLS. The Bouncy Castle LTS modules are version-aligned and isolated inside the provider. Hostname verification is explicitly enabled, and NATS uses DNS-preserving resolution so the intended server name survives into the handshake. Tests reject untrusted certificates, mismatched hostnames and missing client certificates. Plaintext remains supported without certificate settings.

The schema remains owned by Liquibase. A new changeset tunes PostgreSQL outbox/TOAST autovacuum, while JPA marks payload, subject and capture time non-updatable so retries cannot rewrite them. A database trigger in the integration test rejects any retry UPDATE that mentions those columns, including a same-value assignment. ACKed rows are removed immediately; pending events and permanent consumer deduplication records do not receive a destructive time-based trim.
