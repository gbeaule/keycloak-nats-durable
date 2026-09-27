# Dependabot review — 2026-09-26

Reviewed all 13 open Dependabot PRs against `main` at `bce9d694` (including the
NATS restart-readiness fix from PR #14). The update combines the 12 compatible
proposals and retains the Java 21 runtime.

| PR | Dependency | Proposed update | Decision and compatibility considerations |
|---|---|---|---|
| [#1](https://github.com/gbeaule/keycloak-nats-durable/pull/1) | Consumer Temurin image | 21 → 24 | Keep 21 LTS. Java 24 reached end of support in September 2025; it also changes the project's Java 21 runtime baseline. Ignore future Temurin major-version proposals while retaining updates within Java 21. |
| [#2](https://github.com/gbeaule/keycloak-nats-durable/pull/2) | Mockito | 5.18.0 → 5.24.0 | Apply. Test-only dependency; the upstream Android requirement change does not apply to `mockito-core`. |
| [#3](https://github.com/gbeaule/keycloak-nats-durable/pull/3) | setup-java | 4.9.1 → 6.0.1 | Apply using the full release tag. Hosted Ubuntu runners support its Node 24 runtime; the configured Temurin Java 21 input remains supported. |
| [#4](https://github.com/gbeaule/keycloak-nats-durable/pull/4) | JUnit BOM | 5.12.2 → 6.1.3 | Apply with both Maven test runners. JUnit 6 requires Java 17+, satisfied by Java 21; existing tests use Jupiter. Keep the BOM ahead of Keycloak's imported dependency management. |
| [#5](https://github.com/gbeaule/keycloak-nats-durable/pull/5) | Surefire | 3.5.3 → 3.6.0 | Apply with JUnit and Failsafe. The unified JUnit Platform provider supports the existing Jupiter tests. |
| [#6](https://github.com/gbeaule/keycloak-nats-durable/pull/6) | upload-artifact | 4.6.2 → 7.0.1 | Apply using the full release tag. Node 24 is supported; archive mode still defaults to true, preserving the existing multiple-file uploads and artifact names. |
| [#7](https://github.com/gbeaule/keycloak-nats-durable/pull/7) | Maven Enforcer | 3.5.0 → 3.6.3 | Apply. Preserve the Java 21 and Maven 3.9 minimum-version rules. |
| [#8](https://github.com/gbeaule/keycloak-nats-durable/pull/8) | checkout | 4.4.0 → 7.0.1 | Apply using the full release tag. The new restrictions on fork checkout under `pull_request_target` and `workflow_run` do not affect these workflows. Preserve `persist-credentials: false` and read-only permissions. |
| [#9](https://github.com/gbeaule/keycloak-nats-durable/pull/9) | Maven Compiler | 3.14.0 → 3.16.0 | Apply. Keep compilation targeted to Java 21; verify with Maven 3.9.6. |
| [#10](https://github.com/gbeaule/keycloak-nats-durable/pull/10) | Checkstyle | 10.26.1 → 14.1.0 | Apply with source formatting corrections required by the updated Google rules. Keep warnings fatal and retain the full ruleset. |
| [#11](https://github.com/gbeaule/keycloak-nats-durable/pull/11) | Failsafe | 3.5.3 → 3.6.0 | Apply with Surefire and JUnit. Validate real container tests and the method-selection syntax used in PostgreSQL compatibility CI. |
| [#12](https://github.com/gbeaule/keycloak-nats-durable/pull/12) | Spotless | 3.0.0 → 3.10.2 | Apply. Retain the explicit Google Java Format 1.27.0 pin rather than inheriting the newer default formatter. |
| [#13](https://github.com/gbeaule/keycloak-nats-durable/pull/13) | Maven JAR | 3.3.0 → 3.5.1 | Apply. Retain `forceCreation` so incremental builds do not feed previously shaded JARs back into Shade. |

The original PR verification failures are not sufficient evidence of dependency
regressions: the sampled Keycloak 26.7.4 job on every PR except #10 failed while
reconnecting to restarted NATS, before PR #14 fixed that test infrastructure.
PR #10 also failed its security workflow's build with 13 new style violations.
The full reactor exposes 29 violations in total: alignment of 14 text blocks
and the distance between one local variable declaration and its use. The
corrections preserve JSON and SQL contents and event behavior; no style rules
are disabled.

The three actions use full release tags. Dockerfiles, test containers and the
scanner also use version tags, following the repository dependency-reference policy.
No Keycloak, NATS, PostgreSQL, or shipped Java library versions change in this
update.

## Dependency-upgrade validation

Using Java 21.0.10 and Maven 3.9.6:

- `mvn -B -ntp -Psbom verify`: all 388 unit tests passed, with zero failures,
  errors or skips; Checkstyle, Spotless, packaging and both SBOM inventories passed.
- `mvn -B -ntp -Pintegration verify`: all 388 unit tests and 45 container
  integration tests passed, with zero failures, errors or skips. This run uses
  Keycloak 26.7.4 and PostgreSQL 18.6, including the persisted-outbox upgrade from
  Keycloak 26.6.4, and completed in 18 minutes 50 seconds.
- `python scripts/security-check.py`: passed with zero blocking findings. The
  existing policy retains 30 advisory findings in provided dependencies and
  upstream server images; the policy is unchanged.
- `python -m unittest discover -s scripts -p 'test_*.py'`: all five tests passed.
- Parsed all workflow and Dependabot YAML, checked all 13 action references,
  and exercised the existing seven matrix-input validation cases.
- Inspected provider and consumer JARs for Java 21 bytecode, the consumer entry
  point and provider dependency relocation; neither JAR bundles Keycloak,
  Liquibase, JUnit or Mockito classes.

The full GitHub Actions Keycloak/PostgreSQL matrix awaits branch publication.
Local logs are in `.work/dependency-updates-*.log`; reports, inventories, the
security verdict and artifact hashes are in `.work/dependency-update-evidence/`.

## Version-tag follow-up

GitHub Actions, Dockerfiles, Maven test-image defaults and the scanner now use
version tags. The release manifest resolves Dockerfile `ARG` defaults and records
the configured base-image tags. The style guide refers to the POM for tool
versions.

All five container tags were resolved against their registries and matched the
images used in the dependency-upgrade validation above. The follow-up passed
all 388 unit tests, three targeted container tests (`CustomSchemaIT` and
`NatsReadinessIT`), all five Python release-tooling tests, workflow validation,
Checkstyle and Spotless. Logs and image-resolution evidence are in
`.work/dependency-tags-verify.log` and `.work/dependency-tags-images.json`.

## Upstream references

- [Temurin support schedule](https://adoptium.net/support/)
- [JUnit 6.1.3 Java requirements](https://docs.junit.org/6.1.3/overview.html)
- [Surefire 3.6.0 migration guide](https://github.com/apache/maven-surefire/blob/surefire-3.6.0/maven-surefire-plugin/src/site/markdown/whats-new-3-6-0.md)
- [Checkstyle 14.1.0 release](https://github.com/checkstyle/checkstyle/releases/tag/checkstyle-14.1.0)
- [checkout v7.0.1](https://github.com/actions/checkout/tree/v7.0.1)
- [setup-java v6.0.1](https://github.com/actions/setup-java/tree/v6.0.1)
- [upload-artifact v7.0.1 inputs and runtime](https://github.com/actions/upload-artifact/blob/v7.0.1/action.yml)
