# Keycloak → NATS durable events

A Keycloak event listener that publishes to NATS JetStream through a transactional PostgreSQL
outbox. It uses Keycloak's existing database and connection pool. Applications receive events with
any compatible NATS client; the optional consumer example demonstrates transactional deduplication.

**Delivery is at least once, with no global or per-user ordering guarantee.** Selected events commit
with the Keycloak transaction and remain in the outbox until JetStream confirms publication.
Consumers must handle duplicates and acknowledge only after successful processing.

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
docker compose up --build -d
```

Open http://localhost:8080 and sign in as `admin` / `admin-local-only`. Select the `durable-demo`
realm, then disable or delete the `demo` user. Inspect the example consumer's recorded effects:

```sh
docker compose exec postgres psql -U consumer -d consumer -c 'SELECT * FROM knd_effects ORDER BY applied_at'
docker compose logs consumer keycloak
docker compose down
```

[Compose](compose.yaml) defines the demo services and credentials; [the realm fixture](deploy/realm.json)
defines its users and clients. Named volumes preserve state across restarts. The demo uses development
Keycloak mode, plaintext networking and a single broker. See [operations](docs/operations.md) for
deployment requirements.

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

The module POMs enforce initial coverage floors against each module's own tests. Raise them as
meaningful tests close gaps, aiming for 100% of core logic and failure paths. Coverage measures
execution, not assertion quality. To enforce the full 100% unit target locally (currently failing):

```sh
mvn -B -ntp -Dcoverage.line.minimum=1.0 -Dcoverage.branch.minimum=1.0 verify
```

Use complete test runs for coverage, and `mvn clean verify` after removing tests. Integration coverage
is not collected yet: it needs agents in both the host test JVM and Keycloak containers, reports
matching the shaded provider, and collection before deliberate process kills.
