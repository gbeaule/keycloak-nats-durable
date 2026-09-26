# Event contract

Every emitted message has one CloudEvents 1.0 envelope and one of two data shapes: **user** or **admin**. By default, the listener captures every event Keycloak sends to it for an enabled realm. A [hot-reloaded capture policy](configuration.md#choose-events-before-storing-them) can select user event types, admin resources/operations and observed user enablement before events enter the outbox. An enum being listed does not guarantee Keycloak emits it for every operation or external storage provider. Deprecated upstream enums remain representable.

See the [JSON Schema](../schemas/event-v1.schema.json), [complete type/resource catalogue](event-catalogue.md) and [machine-readable catalogue](../schemas/keycloak-catalogue-26.7.4.json). Tests serialize every user event type and every built-in admin resource/operation combination, validate their shape, and check the catalogue against the compiled Keycloak enums. This is shape coverage; real Keycloak integration tests exercise login, failed login, account creation, disablement and deletion.

## NATS subjects

NATS calls a topic a **subject**. With the default prefix:

```text
keycloak.events.<realmToken>.user.<eventTypeLowercase>
keycloak.events.<realmToken>.admin.<resourceToken>.<operationLowercase>
```

| Part | Meaning |
|---|---|
| `keycloak.events` | Configurable `KND_SUBJECT_PREFIX`; the stream owns exactly `<prefix>.>` |
| `realmToken` | Base64url without padding of the UTF-8 **realm ID**, not its display name |
| `user` / `admin` | Keycloak user-event or admin-event family |
| `eventTypeLowercase` | Exact enum name lowercased, including `_error` when present |
| `resourceToken` | Built-in resource enum lowercased, e.g. `user`, `client`, `realm_role`; a custom resource is `custom-` plus base64url of its exact name; missing resource is `unknown` |
| `operationLowercase` | `create`, `update`, `delete` or `action` |

For a realm whose **ID** is `demo`, the realm token is `ZGVtbw`. These patterns can be used as durable consumer filters:

| Filter | Selects |
|---|---|
| `keycloak.events.>` | All events |
| `keycloak.events.ZGVtbw.>` | One realm |
| `keycloak.events.*.user.login` | Successful-login event type across realms |
| `keycloak.events.*.user.login_error` | Failed-login event type across realms |
| `keycloak.events.ZGVtbw.admin.user.*` | USER admin operations in one realm; inspect `outcome` and `userId` |
| `keycloak.events.*.admin.user.delete` | USER deletions across realms; inspect `outcome` before acting |
| `keycloak.events.*.admin.*.update` | All admin UPDATE operations |

Realm IDs are stable across realm renames. Encoding preserves distinctions and prevents `.` / `*` / `>` in input from changing routing. Encoding is **not encryption**. The resource dimension allows account handlers to avoid unrelated admin traffic. Client IDs, user IDs, actor identity and outcome are in the payload, where their absence and differing meanings can be represented accurately. A realm-wide administration event may have no client/user at all; adding these as subject positions would require artificial identifiers and expose more identity information in routing. Filter those fields in a handler when needed. WorkQueue consumers must have non-overlapping subject filters; workers in one application share the same durable consumer.

**Routing change from the initial snapshot:** admin subjects previously ended in `admin.<operation>`. They now end in `admin.<resource>.<operation>`. Update any narrow filters before rollout. Already-persisted outbox subjects and payloads are never rewritten, so drain an earlier backlog first or temporarily subscribe to `<prefix>.>` and filter the data. User subjects and the v1 JSON envelope are unchanged.

## Envelope (common to all events)

```json
{
  "specversion": "1.0",
  "id": "c65be3b2-dc4d-4d68-b380-18c338be1822",
  "source": "urn:keycloak:realm:ZGVtbw",
  "type": "io.keycloak.user.login",
  "time": "2026-09-24T12:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:keycloak-nats:event:v1",
  "data": {
    "kind": "user",
    "realmId": "demo",
    "outcome": "success",
    "eventType": "LOGIN",
    "userId": "user-123",
    "clientId": "web-app"
  }
}
```

The transport `id` is a generated UUID, persisted once and reused unchanged for every retry. It is also the `Nats-Msg-Id` header and the consumer inbox key. `keycloakEventId`, when supplied by Keycloak, is a separate source identifier. `time` is Keycloak's event timestamp in UTC, not commit time or a sequence number. The NATS `Content-Type` header is `application/cloudevents+json`; the embedded data content type is `application/json`.

For user events, envelope `type` is `io.keycloak.user.<eventTypeLowercase>`. For admin events it is `io.keycloak.admin.<operationLowercase>`; the resource type is a subject dimension and payload field. Keeping envelope type stable allows one schema per family rather than a new schema for every resource enum.

## Complete data shapes

All present values have the JSON type below. Optional fields are omitted, **never JSON null**. There are no undocumented payload fields.

| Field | JSON type | Presence / meaning |
|---|---|---|
| `kind` | string | Required: `user` or `admin` |
| `realmId` | string | Required: affected realm ID |
| `outcome` | string | Required: `success` if Keycloak supplied no error, otherwise `error` |
| `keycloakEventId` | string | Optional source event ID |
| `error` | string | Present only for an error outcome; Keycloak's error value |
| `eventType` | string | Required for user events only; exact uppercase enum |
| `operationType` | string | Required for admin events only; exact uppercase enum |
| `resourceType` | string | Admin only, if supplied; built-in uppercase enum or custom name |
| `resourcePath` | string | Admin only, if supplied; Keycloak resource path |
| `userId` | string | User events: event's subject user if known. Admin: target ID only for resource `USER` and exact path `users/<id>` |
| `clientId` | string | User events: originating client. Admin: acting client's identifier from `AuthDetails`; it is **not** necessarily the client being edited |
| `actorUserId` | string | Admin only: authenticated actor if supplied |
| `actorRealmId` | string | Admin only: actor's realm if supplied, possibly different from the affected realm |
| `userEnabled` | boolean | Successful direct `USER` CREATE/UPDATE only, when the user model is available; observed state, not a transition |

### User success, user error, and missing identity

[Login](examples/user-login.json), [login error](examples/user-login-error.json) and [minimal user event](examples/user-minimal.json) are complete envelopes. All user enums use these same shapes. For example:

```json
{"kind":"user","realmId":"demo","outcome":"error","eventType":"LOGIN_ERROR","clientId":"web-app","error":"invalid_user_credentials"}
```

A failed login can have no known `userId`. Service/client operations can also omit user identity. `_ERROR` is part of the event type; `outcome` is independently derived from the supplied error field, not guessed from the enum name.

### Admin creation, enablement, disablement and deletion

Complete examples: [create](examples/admin-create.json), [disable](examples/admin-disable.json), [enable](examples/admin-enable.json), [delete](examples/admin-delete.json).

```json
{"kind":"admin","realmId":"demo","outcome":"success","resourceType":"USER","operationType":"UPDATE","resourcePath":"users/user-123","userId":"user-123","userEnabled":false,"actorUserId":"admin-456","actorRealmId":"master","clientId":"admin-cli"}
```

`userEnabled: false` means disabled state was observed during this successful UPDATE. It does not prove this particular update changed enablement: a profile edit on an already-disabled user produces the same observation. There is no invented `USER_DISABLED` or `USER_ENABLED` event. DELETE carries the target ID from the path and omits `userEnabled`; no lookup of the deleted user is needed.

### Other admin operations, errors and resource paths

[ACTION](examples/admin-action.json), [error](examples/admin-error.json), [nested resource](examples/admin-nested-resource.json), [custom resource](examples/admin-custom-resource.json) and [minimal admin](examples/admin-minimal.json) illustrate the remaining shapes. CREATE, UPDATE, DELETE and ACTION all use the admin shape above. Keycloak's admin operation enum has no READ value; ordinary GET requests do not become read-audit events through this listener.

Admin errors include `error` and omit `userEnabled`; they can still identify the attempted target. A nested path such as `users/user-123/role-mappings/realm` is not treated as a direct account change and has no inferred `userId` or `userEnabled`. `resourceType` can be an extension-defined string; consumers must tolerate new values. Do not parse every resource path using the `users/<id>` rule.

## Processing boundaries

Redelivery can occur before or after another event for the same user. There is no global, per-realm or per-user order guarantee. An enabled-state projection must reconcile with current Keycloak state; replaying timestamps in arrival order is not sufficient to build a reliable authorization cache.

The provider omits representations, arbitrary event details, email, username, IP addresses, passwords and tokens. IDs, resource paths and error strings still carry identity/operational information and require access controls. No event is synthesized for direct SQL, an external LDAP change, a storage-provider operation that emits no Keycloak event, a disabled listener, or an event before installation. Temporary brute-force lockout is not the same as an administrator setting `enabled=false`; handle the actual Keycloak event types where emitted.
