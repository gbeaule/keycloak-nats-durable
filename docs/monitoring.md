# Owner-managed delivery monitoring and capacity

The Keycloak/application owner controls admission policy, storage budgets, monitoring frequency,
alert routing and recovery. The provider retains accepted events until a confirmed publish. It does
not evict the outbox or make Keycloak readiness depend on NATS availability.

Use [capture scopes](configuration.md#choose-events-before-storing-them) to exclude unwanted realms,
clients, outcomes, topics and auth/admin events **before** they consume space. A capture policy change
does not remove existing backlog. Use [consumer recovery policies](consumer.md) when an application
explicitly accepts quarantine or loss. Default processing never drops messages.

## Run the read-only collector

The consumer artifact includes a standalone outbox collector; it does not require a NATS connection or
consumer schema. Give it a separate monitoring identity on the existing Keycloak database:

```sql
-- Adapt the role and schema to your deployment; provision its credentials through your secret store.
GRANT USAGE ON SCHEMA public TO knd_monitor;
GRANT SELECT (subject, created_at, next_attempt_at, attempts)
  ON public.kc_nats_outbox TO knd_monitor;
```

Set `KND_OUTBOX_DB_URL`, `KND_OUTBOX_DB_USER`, `KND_OUTBOX_DB_PASSWORD`, and optionally
`KND_OUTBOX_DB_SCHEMA` (default `public`). The collector starts a read-only transaction, reads no event
payloads, and bounds connection, socket, statement and lock waits. Run it once per database:

```sh
java -cp consumer-example/target/consumer-example-1.0.0-SNAPSHOT.jar \
  io.github.gbeaule.keycloaknats.consumer.OutboxReport json

java -cp consumer-example/target/consumer-example-1.0.0-SNAPSHOT.jar \
  io.github.gbeaule.keycloaknats.consumer.OutboxReport prometheus
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `KND_REPORT_TIMEOUT_SECONDS` | `5` | Connection/socket/statement limit, 1–300 |
| `KND_REPORT_SUBJECT` | `>` | A full NATS subject pattern; scope exact counts by topic |
| `KND_REPORT_REALM_ID` | unset | Exact realm ID; encoded into the subject's realm token |
| `KND_SUBJECT_PREFIX` | `keycloak.events` | Routing prefix used with the realm selector |
| `KND_REPORT_MAX_ROWS` | `0` | Owner threshold; zero disables it |
| `KND_REPORT_MAX_AGE_SECONDS` | `0` | Owner threshold; zero disables it |

The report contains selected pending/due counts, oldest age, maximum attempts, total table/index/TOAST
bytes, estimated dead tuples and last autovacuum time. Storage and vacuum values cover the entire
outbox even when the event counts are scoped. It also exposes collection success and timestamp.
Counts are exact scans; a large backlog can exceed the query deadline. Adjust frequency and limits
from measured database cost, and use PostgreSQL estimates between full counts when needed.

Exit status is `0` for a successful report within thresholds, `1` when an enabled threshold is met or
exceeded, and `2` for configuration/collection failure. A failed collection still emits a failure
metric. No action is taken against Keycloak or its backlog. Schedule this command in your monitoring
agent or job runner. For a Prometheus textfile collector, write output to a temporary file in the
collector directory and atomically replace the `.prom` file after the command completes; preserve
failure output as well as success output. Alert on the collection timestamp and missing collector
jobs so stale files cannot appear healthy.

The [example Prometheus rules](../deploy/prometheus-rules.yml) are starting points to adapt to your
delivery SLO. Scrape [consumer metrics](consumer.md#owner-monitoring) separately. Existing PostgreSQL
and NATS monitoring should cover disk/WAL headroom, replication health, pool occupancy, blocked
vacuum, stream bytes, consumer pending/ACK-pending/redelivery counts and quorum. An empty outbox
proves neither consumer completion nor sufficient recovery capacity. Add an end-to-end canary for a
dedicated test realm and verify its effect within your delivery deadline.

## Plan outage capacity

Measure accepted events per second and bytes consumed per pending row using your own event mix and
database settings. Include indexes, TOAST, WAL, replication slots, dead tuples and operational
headroom. A useful starting budget is:

```text
outbox budget ≈ accepted events/second × outage seconds × measured bytes/event × safety factor
catch-up seconds ≈ backlog events / (sustained relay rate − ongoing accepted event rate)
```

Catch-up requires a positive denominator. The broker and consumer must also sustain that rate.
Measure database pool pressure and login/admin p95/p99 while draining. Configure additional relay
workers with `KND_RELAY_WORKERS` only within the available pool/storage capacity. Use alerts early
enough to expand capacity, narrow future capture under an approved policy, or stop accepting relevant
operations before the shared database fills. Keep pending outbox and inbox data intact; normal vacuum
reuses deleted-row space without requiring destructive trimming.
