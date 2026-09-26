# Throughput and outage capacity

The opt-in benchmark uses real Keycloak login/admin HTTP requests, PostgreSQL durable commits and
JetStream File storage with synchronous writes. It measures request latency, capture rate, outage
backlog bytes, concurrent capture during recovery and consumer transaction-plus-ACK throughput. It
asserts that all persisted backlog IDs arrive and that every unique ID has one transactional effect.

```sh
mvn -B -ntp -Pperformance verify \
  -Dbenchmark.events=500 -Dbenchmark.concurrency=8 \
  -Dbenchmark.warmup=20 -Dbenchmark.relay.workers=1,4
```

Reports are written to `integration-tests/target/performance-workers-N.json`. Run performance tests
separately from functional tests and other heavy workloads. Increase event counts and warmup for a
steady-state measurement. The reference uses Keycloak production `start`, a database pool maximum of
32, one broker replica and one consumer worker; it is not a production three-replica capacity result.

## Initial measurement, 2026-09-26

Windows 11 / Docker Desktop, Java 21.0.10, 16 reported host processors, Keycloak 26.7.4, PostgreSQL
18.6 (`fsync=on`, `synchronous_commit=on`), NATS 2.12.8 (`sync_interval: always`), eight concurrent HTTP
requests. Each configuration captured 500 events live, 500 during a broker outage and 250 while the
broker recovered. The benchmark alternated password logins and user creation, after 20 warmup requests.

| Measurement | 1 relay worker | 4 relay workers |
|---|---:|---:|
| Live requests/second | 78.1 | 77.2 |
| Login p95 / p99, ms | 171.1 / 205.5 | 180.3 / 205.0 |
| Admin p95 / p99, ms | 91.1 / 107.7 | 93.7 / 107.7 |
| Broker restart and drain of 750 events, seconds | 7.71 | 5.09 |
| Recovery delivery rate, events/second | 97.3 | 147.2 |
| One pooled consumer, live / recovery events/second | 80.3 / 76.3 | 76.5 / 86.2 |

Adding a bounded JDBC pool increased the same single consumer from approximately 33–35 to 76–86
events/second in these runs. Four independent relay publishers improved recovery throughput by about
51% in the pooled run. Small differences in request latency do not establish a regression or guarantee:
these are single workstation runs with short warmup, no injected WAN latency and no confidence intervals.
NATS was subsequently updated to 2.15.0 for security; remeasure on the approved deployment versions.

## Choose concurrency and capacity from the deployment

`KND_RELAY_WORKERS` defaults to 1, with a range of 1–16 per Keycloak node. Each active relay can hold one
Keycloak database connection across bounded NATS requests. Reserve pool capacity for authentication,
admin work and recovery; increasing workers without that headroom can increase request latency.
`KND_CONSUMER_DB_POOL_SIZE` defaults to 1, sufficient for the shipped one-at-a-time worker. Scale consumer
replicas against the shared inbox and durable; their shared MaxAckPending limit does not multiply.

Measure database pool occupancy/waits and `pg_stat_activity`, HTTP error rate and p95/p99, outbox age,
WAL/storage growth, broker quorum/storage, pending-window saturation and consumer progress together.
The benchmark reports configured pool size, not direct Keycloak pool occupancy. Use the deployment's
Keycloak metrics and PostgreSQL monitoring for occupancy. The [collector and alert templates](monitoring.md)
provide backlog measurements without reading payloads or assigning an admission policy.

Let capture rate be `C`, sustained relay rate `R`, and initial backlog `B`. Recovery requires `R > C`;
the ideal drain time is `B / (R - C)`, before restart delays, retries and downstream limits. Consumers
must also exceed the long-term incoming rate. Budget outage storage using observed row/index/TOAST and
WAL growth, vacuum delay and a safety margin, rather than JSON size alone. Run an outage lasting the
owner's required window and prove recovery while traffic continues before adopting capacity numbers.
