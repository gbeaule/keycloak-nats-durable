# Keycloak → NATS durable events

A Keycloak event listener that publishes to NATS JetStream through a transactional PostgreSQL
outbox. It uses Keycloak's existing database and connection pool. Applications receive events with
any compatible NATS client; the optional consumer example demonstrates transactional deduplication.

**Publication is ordered per affected user across Keycloak nodes.** Selected events commit with the
Keycloak transaction and retry until JetStream confirms acceptance, unless their captured policy
explicitly permits local discard. A pending event blocks later publication for that user; other users
can progress. Sequence gaps are valid, and a timeout can leave an accepted or in-flight original.
Receiving applications own deduplication and processing order.

## Start here

- [Architecture](docs/architecture.md): transaction boundaries, delivery guarantees and tradeoffs.
- [Receiving events](docs/events.md): routing, event meaning and consumer responsibilities.
- [Operations](docs/operations.md): installation, configuration, capacity and recovery.
- [Development](docs/development.md): validation, compatibility and release policy.

Implementation details live in the code, schemas, build files and tests linked from these guides.

## Build and try locally

Use Java 21 and Maven 3.9 or later. Docker is required for the demonstration and integration tests.
Dependency versions and build requirements are defined in [pom.xml](pom.xml).

```sh
mvn -B -ntp verify
docker compose -f compose.producer.yaml up --build -d
```

Open http://localhost:8080 and sign in as `admin` / `admin-local-only`. Select the `durable-demo`
realm, then disable the `demo` user. Inspect the publisher without creating a consumer:

```sh
docker compose -f compose.producer.yaml run --rm provision nats --server=nats://nats:4222 stream info KEYCLOAK_EVENTS
docker compose -f compose.producer.yaml exec postgres psql -U keycloak -c 'SELECT count(*) FROM kc_nats_outbox'
docker compose -f compose.producer.yaml logs keycloak
docker compose -f compose.producer.yaml down
```

[Producer Compose](compose.producer.yaml) defines the standalone services and credentials;
[the realm fixture](deploy/realm.json)
defines its users and clients. Named volumes preserve state across restarts. The demo uses development
Keycloak mode, plaintext networking and a single broker. See [operations](docs/operations.md) for
deployment requirements.

The [consumer demonstration](compose.yaml) is optional. The standalone deployment
creates only a stream, with no subscription or consumer database. An empty outbox means publication
obligations are resolved, including authorized discards; it does not establish downstream completion.

The installable provider is `extension/target/keycloak-nats-durable-<version>.jar`; the runnable example
is `consumer-example/target/consumer-example-<version>.jar`. Use the version from your build.
Provider installation does not require the example consumer or its database.

## Validate

```sh
python3 scripts/validate.py              # unit tests, style, packaging and Python tooling
python3 scripts/validate.py integration  # container tests; requires Docker
```

Use `python` instead of `python3` on Windows. See [development](docs/development.md) for the
compatibility matrix, performance checks and release workflow.

[JaCoCo](https://www.jacoco.org/jacoco/trunk/doc/maven.html) measures unit-test line and branch
coverage. After `verify`, open `coverage/target/site/jacoco-unit/index.html` for the combined report,
including shared code exercised by other modules' tests. Per-module reports use the same path under
each module's `target`. CI uploads these reports as `unit-coverage`; no production classes are excluded.

Module POMs define coverage gates against each module's own tests. The provider and shared transport
require full line and branch coverage; the consumer example has a lower floor. Coverage measures
execution, not assertion quality. To apply the full target to every module locally:

```sh
mvn -B -ntp -Dcoverage.line.minimum=1.0 -Dcoverage.branch.minimum=1.0 verify
```

Use complete test runs for coverage, and `mvn clean verify` after removing tests. Integration coverage
is not collected yet: it needs agents in both the host test JVM and Keycloak containers, reports
matching the shaded provider, and collection before deliberate process kills.
