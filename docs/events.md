# Receiving events

Applications consume ordinary NATS JetStream messages with a CloudEvents envelope. The provider
captures user and admin events emitted to the listener in enabled realms, subject to the deployment's
capture policy. A listed event type describes a supported shape, not a promise that every operation
or external storage provider emits it.

The exact contract lives in the [versioned JSON schema](../schemas/event-v1.schema.json),
[machine-readable Keycloak catalogue](../schemas/keycloak-catalogue-26.7.4.json) and
[envelope implementation](../extension/src/main/java/io/github/gbeaule/keycloaknats/EventEnvelope.java).
[JSON examples](examples) are validated by
[schema tests](../extension/src/test/java/io/github/gbeaule/keycloaknats/EventSchemaTest.java).

## Routing and identity

Subjects follow these shapes:

```text
<prefix>.<realmToken>.user.<event>
<prefix>.<realmToken>.admin.<resource>.<operation>
```

The realm token encodes the immutable realm ID as UTF-8 base64url without padding. Built-in event,
resource and operation names are lowercased. Encoding protects subject boundaries; it is not
encryption. With the default prefix, `keycloak.events.*.user.login` selects logins across realms and
`keycloak.events.*.admin.user.update` selects USER updates.

The envelope `id` is the persisted delivery identity, reused as `Nats-Msg-Id` on every retry. Deduplicate
with this ID, not the optional Keycloak source-event ID. Timestamps do not establish processing order.
Check `outcome` and the affected identity before applying a business effect.

User events include `data.sessionId` when Keycloak supplies a user session ID. Use it to correlate
login, token refresh and logout for the same session. A login can reuse an existing SSO session;
it does not necessarily create one. Events without a session ID omit the field. Admin events do not
expose this field; a targeted session deletion identifies the session in `data.resourcePath`.

Consumer subject filters select stored messages. Capture filters determine which events enter the
pipeline at all. A consumer filter does not remove unselected messages from the stream.

An attributable user's envelope includes `data.ordering.key` and a positive decimal-string
`data.ordering.sequence`. These describe capture order across nodes, independently of the event timestamp.
The relay attempts a successor only after earlier positions resolve by publication or authorized
local discard. Sequence gaps are valid, including with filtered subscriptions. Unattributable events
omit ordering metadata and publish independently; no receiver library or full-feed subscription is
required. Subjects are unchanged.

Publication policy and database capture time are internal snapshots. They are not subscriber
processing deadlines. Local discard emits no message and cannot retract a previously accepted or
in-flight original; a timeout is not proof that NATS received nothing. Consumers must tolerate
duplicates and possible late originals after ambiguous discard.

## Interpret account events carefully

[Disablement](examples/admin-disable.json) is a successful direct USER update with
`data.userEnabled: false`. It can also describe another edit to an already disabled user.
[Deletion](examples/admin-delete.json) identifies the target from the direct user resource path.
[Recognized nested resources](examples/admin-nested-resource.json) identify the affected user and
share that user's capture sequence. Only direct USER create/update events observe `userEnabled`.

User identity can be absent, including on a [failed login](examples/user-login-error.json).
For admin events, the actor and affected user are distinct. A client identifier can describe the
acting client rather than a resource being edited. Consume optional fields accordingly and reconcile
state projections as described in [architecture](architecture.md#ordering-and-event-meaning).

The envelope omits raw representations and arbitrary event details. Remaining identifiers, resource
paths and error values still require access controls and an appropriate retention policy.

## Consumer responsibilities

Use durable consumers with explicit acknowledgements and unlimited redelivery. Acknowledge only
after processing commits. Replicas of one logical application share its durable consumer and
deduplication state. Independent applications need separate durables and a retention plan that keeps
each event until all required applications have processed it.

The [consumer example](../consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer)
demonstrates an inbox and database effect committed together, followed by a confirmed ACK.
Its effect ledger is a demonstration, and installing the provider does not require it. Your application
owns its business processing, deadlines, idempotency and recovery policy.

Keep inbox identities for the full possible replay horizon, including backups and restores. Removing
them merely because they are old can repeat effects. An external HTTP or email effect cannot be made
exactly once by the example's database transaction.

Failed processing retries by default. The example also offers explicit subject-scoped quarantine and
discard policies. Quarantine preserves a recovery copy before ACK; discard retains audit metadata
without a replayable payload. See [operations](operations.md#consumer-recovery) before enabling either.
