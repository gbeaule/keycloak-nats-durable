# Capture policy, TLS and HA

## Configuration ownership

The shared [`Environment`](../nats-transport/src/main/java/io/github/gbeaule/keycloaknats/config/Environment.java)
is the only process-environment reader. The provider's names, defaults and Keycloak SPI precedence
live in [`BridgeConfig`](../extension/src/main/java/io/github/gbeaule/keycloaknats/BridgeConfig.java).
The separate example consumer, provisioner and collector load their names and defaults through
[`ConsumerConfig`](../consumer-example/src/main/java/io/github/gbeaule/keycloaknats/consumer/ConsumerConfig.java).
They load settings at startup; runtime classes receive typed, validated values. The shared reader
preserves the platform's native environment-name lookup, including case insensitivity on Windows.
Keeping the two application loaders separate avoids making a NATS receiver depend on Keycloak APIs.
Worker policy records validate values but delegate environment loading to that central consumer loader.

## Choose events before storing them

The capture policy is optional. Leave `KND_FILTER_FILE` unset to capture all events emitted to this listener. To exclude irrelevant events before they use outbox or broker space, set `KND_FILTER_FILE=/etc/knd/events.json` and mount that file's **directory** read-only. `KND_FILTER_RELOAD_MS` defaults to 1000 (range 100–60000). Only the capture policy is hot-reloaded; connection, routing and relay settings require a restart.

The `config/events-*.json` files, including `events-scoped.json`, are optional examples in the source
repository. They are not packaged in the JARs, images or candidate bundle, or loaded automatically.
Copy/edit a policy and supply its path explicitly. The scoped example's realm ID is a placeholder.

For successful direct USER updates observed with `enabled=false`, use [events-disabled-only.json](../config/events-disabled-only.json):

```json
{
  "userEvents": [],
  "adminEvents": [
    {"resourceType": "USER", "operations": ["UPDATE"], "userEnabled": false}
  ]
}
```

Keycloak emits USER UPDATE, without a separate disable-transition event. This policy also includes other updates to an already disabled user. It excludes failed operations and events without a direct user state observation. Include `CREATE` as well if users created disabled matter. Detecting actual transitions requires a separate transactional state history; arrival order is insufficient.

Both top-level arrays are required. Empty arrays exclude that category. `userEvents` accepts exact uppercase Keycloak event names, or `["*"]` for all. Each admin rule requires `resourceType` and `operations`. Rules are combined with OR; conditions in a rule are combined with AND. Resource types accept an exact uppercase built-in name, `*`, or `custom:NAME` for a custom resource. Operations accept `CREATE`, `UPDATE`, `DELETE`, `ACTION`, or `["*"]`. Optional `userEnabled` must be a boolean and is allowed only for USER CREATE/UPDATE. Unknown fields, misspelled built-ins, duplicate keys/values, mixed wildcard lists, trailing JSON, missing arrays and files over 64 KiB are rejected. [events-all.json](../config/events-all.json) shows an explicit all-events policy.

Optional top-level scope arrays narrow those rules further. Every specified dimension must match;
omitted dimensions include everything and an empty array excludes everything. Existing policies keep
their existing meaning.

| Field | Matches |
| --- | --- |
| `realmIds` | Exact, immutable Keycloak realm IDs, or `["*"]` |
| `clientIds` | User event client ID or admin actor's client ID, or `["*"]`; a missing client matches only the wildcard |
| `outcomes` | `success`, `error`, or `["*"]` |
| `subjects` | Full NATS subject patterns; `*` matches one token and terminal `>` matches one or more |

Use [events-scoped.json](../config/events-scoped.json) as a starting point for a realm and event-family
policy. Realm names are not IDs; read the realm's `id` from the admin API. Subject realm tokens use the
existing encoded ID contract, so `realmIds` is usually clearer for selecting named tenants while
`subjects` selects topics such as `keycloak.events.*.user.login`. These are capture-time filters;
realm/client IDs and topics are not automatically added as monitoring labels. Scope checks happen
before envelope serialization and admin user-state lookups. The policy remains bounded to 64 KiB.

Write a complete replacement in the same directory and atomically rename it over `events.json`. A background thread reopens the path and compares content, so same-timestamp replacements and projected-volume symlink changes are detected. Avoid Kubernetes `subPath` mounts, which do not receive projected updates. Each callback uses one immutable snapshot. Excluded events are neither serialized nor inserted, and excluded admin categories avoid user lookups. Changes affect future capture; previously committed outbox events always drain using their original subject and bytes.

A missing or invalid initial file fails provider initialization. A missing, unreadable or invalid replacement keeps the last valid policy and logs an error. Recovery logs the applied SHA-256 digest. All nodes must receive the same policy; compare digests across their logs. Reloads are local and eventually consistent, not a cluster-wide atomic switch. For an exact policy cutover, quiesce writes while deploying and verifying the policy on every node.

To return a running filtered deployment to capture-all, replace the file with [events-all.json](../config/events-all.json). Deleting the configured file retains the last valid policy; it does not reset to capture-all. Unsetting `KND_FILTER_FILE` takes effect after a restart.

## Subjects and streams

Keep one stream covering `<prefix>.>` and the existing event-specific subjects:

| Subscription | Events |
|---|---|
| `keycloak.events.*.user.login` | Logins in every realm |
| `keycloak.events.*.admin.user.update` | USER updates in every realm |
| `keycloak.events.*.admin.user.*` | All direct USER operation types |

Capture filters control what enters PostgreSQL/NATS; consumer subject filters control which stored messages a consumer receives. A disabled-only policy still publishes to `.admin.user.update`, with `data.userEnabled=false`. There is no new synthetic event type or changed v1 subject contract. Separate streams are useful for distinct retention or access requirements, but this provider binds its whole prefix to one validated stream. WorkQueue retention permits non-overlapping consumer filters; independent applications needing the same event require Limits retention and their own durable consumers, with coordinated pruning.

### When nobody is listening

The publisher sends to a JetStream stream and does not check for connected workers. The default WorkQueue stream retains messages until a consumer acknowledges them, including messages published before any consumer is created. A durable consumer also retains pending delivery while all its workers are offline. A consumer's subject filter does not delete other subjects from the stream.

Core NATS drops publications with no interested subscriber. JetStream Interest retention can remove messages immediately when no matching consumer is defined; a defined durable consumer still expresses interest while its workers are offline. The provider rejects Interest retention because durable capture must survive consumers being created later. These distinctions follow [NATS retention semantics](https://docs.nats.io/nats-concepts/jetstream/streams#retentionpolicy).

Keep capture-all when all event types need durable handling or replay, and give the retained events an eventual processing/retention plan. Use the optional capture policy for events the deployment will never need. With the configured DiscardNew policy, an unconsumed stream eventually fills and new publications remain in PostgreSQL for retry. Filtering by currently connected workers would confuse a temporary outage with permanent irrelevance and could lose needed events.

Replacing the capture policy changes future capture only. Previously stored outbox or stream messages remain pending even if the new policy excludes their types; replacing a policy is not a backlog purge.

## Mounted TLS certificates

TLS is optional. Publisher, example provisioner and example consumer accept plaintext servers such as `KND_NATS_URL=nats://nats:4222` without any `KND_TLS_*` settings. No certificate files are needed for plaintext connections.

To enable TLS, use `tls://` URLs. All three applications share the same TLS implementation:

```text
KND_NATS_URL=tls://nats1:4222,tls://nats2:4222,tls://nats3:4222
KND_TLS_CA_FILE=/etc/knd/tls/ca.pem
KND_TLS_CERT_FILE=/etc/knd/tls/client.pem
KND_TLS_KEY_FILE=/etc/knd/tls/client.key
KND_MIN_REPLICAS=3
```

Mount the TLS directory read-only and allow the process UID to read the private key. `tls-ca-file` is a PEM CA bundle; when omitted, JVM trust roots apply. The client certificate file holds the leaf certificate followed by any intermediates. Its paired key file must contain exactly one unencrypted PEM private key: PKCS#8 (`BEGIN PRIVATE KEY`), RSA PKCS#1 (`BEGIN RSA PRIVATE KEY`) or EC SEC1 (`BEGIN EC PRIVATE KEY`), with an algorithm supported by the JDK. Encrypted keys must be converted before deployment; this configuration has no key-password setting. Files are bounded to 1 MiB each. TLS-only server authentication omits both client files. NATS token or JWT/NKey credentials remain separate from TLS and can be combined with it.

Private-key parsing and conversion use Bouncy Castle's maintained `PEMParser` and `JcaPEMKeyConverter`, rather than local PEM/Base64 parsing. The `bcprov`, `bcutil` and `bcpkix` dependencies use the same pinned LTS version, including NATS's provider dependency. The provider JAR relocates their packages to avoid conflicts with Keycloak. Certificate loading, trust-chain checks, key managers and TLS stay with the JDK; no JVM-wide security provider is registered. See [Bouncy Castle LTS](https://www.bouncycastle.org/download/bouncy-castle-java-lts/).

For example, merge this service fragment into your deployment and give every Keycloak node the same policy mount:

```yaml
services:
  keycloak:
    environment:
      KND_NATS_URL: tls://nats1:4222,tls://nats2:4222,tls://nats3:4222
      KND_TLS_CA_FILE: /etc/knd/tls/ca.pem
      KND_TLS_CERT_FILE: /etc/knd/tls/client.pem
      KND_TLS_KEY_FILE: /etc/knd/tls/client.key
      KND_FILTER_FILE: /etc/knd/config/events-disabled-only.json
    volumes:
      - ./config:/etc/knd/config:ro
      - ./secrets/nats:/etc/knd/tls:ro
```

Every seed must use `tls://` when TLS is selected. Mixed plaintext/TLS seeds and certificate files on plaintext connections are rejected. The client verifies both the trust chain and the hostname, including discovered servers. Certificates need SANs matching the addresses advertised by NATS; an IP address needs an IP SAN. There is no trust-all or skip-hostname option. Certificate validation failure retains the outbox and retries. Connection timeouts also bound the TLS connection attempt.

Certificates are loaded when a client connection is constructed. Reconnects on an existing NATS connection reuse its TLS context. Rotate files as a complete bundle, then roll the Keycloak/consumer processes to load them; certificate hot reload is not implied by policy hot reload. Configure server-side TLS and authorization separately. For a NATS cluster, also secure route connections and use node certificates suitable for both client and server authentication. See [NATS TLS configuration](https://docs.nats.io/learn/security/encryption).

## Cluster deployment

Run at least two Keycloak nodes with the identical provider JAR against one HA PostgreSQL endpoint. Start production Keycloak with distributed caching (`start --cache=ispn`, with `jdbc-ping` for the tested versions), a shared external hostname, and an appropriately configured load balancer. Permit the cache transport between nodes. The bridge's SQL row locking does not replace Keycloak's cache invalidation and session clustering. See [Keycloak distributed caches](https://www.keycloak.org/server/caching).

Use three NATS servers, independent persistent storage/failure domains, File storage, three stream replicas, durable consumer state, no expiry, and DiscardNew. Provide multiple reachable TLS seed URLs; discovered addresses must also be reachable. A three-replica stream tolerates one failed replica. Losing a majority stops confirmed publication, so events remain in PostgreSQL until quorum recovers. A dead Keycloak node releases database locks when its connection is detected as lost; another node resumes those rows with the original IDs. Do not deploy different NATS accounts, stream names, subject prefixes or filters to nodes sharing an outbox.

The bridge does not provision PostgreSQL replication, a load balancer, NATS routes or storage. Use a synchronous PostgreSQL HA configuration consistent with your loss budget, with fencing to prevent two writable primaries. Asynchronous database failover may lose acknowledged account/outbox transactions. Verify failover, backup restore, capacity, network timeouts and credentials in the target environment. The repository's failure tests cover the bridge's recovery through NATS quorum loss and Keycloak process failure; they do not certify a database failover product.
