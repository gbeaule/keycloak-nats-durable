# Development

## Build and validate

Use Java 21, Maven 3.9 or later and Python 3.9 or later. Check `mvn -version` to confirm Maven uses the
intended JDK. Docker must be running for container tests and security scans.

```sh
python3 scripts/validate.py              # quick checks; no Docker required
python3 scripts/validate.py integration  # full suite on the default runtime
python3 scripts/validate.py matrix       # configured Keycloak and PostgreSQL selections
python3 scripts/validate.py security     # fresh inventories and dependency/image scans
mvn spotless:apply                      # format Java
```

Use `python` on Windows. [validate.py](../scripts/validate.py) defines available modes and options;
`--help` lists focused runs and Maven argument forwarding. Google Java formatting and style checks
are enforced by [pom.xml](../pom.xml); [.editorconfig](../.editorconfig) supplies editor defaults.

Tests are the source of truth for covered scenarios. Unit tests live in each module's `src/test`;
[container tests](../integration-tests/src/test/java/io/github/gbeaule/keycloaknats) exercise the
packaged application. Integration runs fail when Docker is unavailable. Inspect JUnit reports under
each module's `target` directory and container logs under `integration-tests/target`.

Run checks appropriate to the change. Delivery, database, upgrade and dependency changes need the
compatibility matrix; dependency and deployment image changes also need fresh security scans.
The `performance` Maven profile runs the
[opt-in benchmark](../integration-tests/src/test/java/io/github/gbeaule/keycloaknats/ThroughputBenchmark.java).
Measure capacity and recovery on the intended deployment; reference tests do not certify its topology.

## Compatibility

The [root POM](../pom.xml) defines compile and default runtime versions.
[config/keycloak-versions.json](../config/keycloak-versions.json) defines Keycloak runtime targets;
[validate.py](../scripts/validate.py) defines PostgreSQL selections and focused coverage. These files
describe the test matrix. Passing reports for the relevant source commit provide the evidence.

The provider depends on Keycloak transaction/lifecycle APIs, custom JPA registration and Hibernate
locking behavior. A successful compilation does not establish compatibility with a new runtime,
another database engine, a vendor distribution or a mixed-version rolling upgrade.

For a candidate Keycloak release, run `validate.py matrix --keycloak-version X.Y.Z`, replacing
`X.Y.Z` with the exact release. This adds a candidate to the run without changing the checked-in
matrix. Before adding it to that matrix:

1. Review changes to listener transactions, entity registration, migrations, durable commits,
   row locking and event enums.
2. Run the full existing matrix, including [persisted-outbox upgrade tests](../integration-tests/src/test/java/io/github/gbeaule/keycloaknats/UpgradeIT.java).
   If the compile baseline changes, validate every existing runtime against it.
3. Review the event schema, catalogue and examples together. Keep their
   [enum/schema checks](../extension/src/test/java/io/github/gbeaule/keycloaknats/EventSchemaTest.java)
   meaningful and preserve existing event identities and routing during upgrades.

Apply the selected server's own support and upgrade requirements. Test the deployment's exact
source/target versions on a restored database; default test images do not require existing
installations to change their database major version.

Add new database changesets for schema evolution. Preserve applied migration identities and the
persisted event contract; rewriting old migrations or pending payloads undermines recovery.

## CI and release trust

[Verification](../.github/workflows/verify.yml) runs quick PR checks, with broader compatibility checks
on qualifying pushes to `main` and manual runs. Workflow files define triggers, job selections and
artifact retention. Require `quick-checks` for PRs through repository settings, and require passing
matrix and security evidence before release.

PR builds execute untrusted code. Keep them on disposable hosted runners without production secrets,
write tokens or access to private infrastructure. Repository administrators own fork approval,
branch/tag protection and secret-sharing settings. Review workflow and build changes, and rebuild
release artifacts from reviewed source rather than promoting PR artifacts.

The [release-candidate workflow](../.github/workflows/release-candidate.yml) builds and tests fresh
artifacts, generates inventories, scans dependencies/images and packages a checksummed bundle. It
does not publish a release. The [manifest tool](../scripts/release-manifest.py) records artifact
identity and source revision. Retain the bundle, resolved image identities, test evidence and security
reports with each deployment; version tags and checksums alone do not prove provenance.

## Security and dependency updates

The [security workflow](../.github/workflows/security.yml) and [scan tool](../scripts/security-check.py)
define the current gate. The policy blocks fixable HIGH/CRITICAL findings in shipped runtime
dependencies. Upstream image and provided Keycloak dependency findings are owner-accepted advisory
findings, retained at their reported severity. This distinction does not establish that those
findings are harmless. Scanner failures and incomplete runtime inventories fail validation.

Use fresh scan reports in `target/security` and their `gate.json` verdict for the candidate being
reviewed. Inventories must come from the same build; Maven offline mode skips inventory generation.
Consume upstream servers as released and follow their update path rather than replacing internal
libraries through the provider.

[Dependabot configuration](../.github/dependabot.yml) defines update proposals. Review them with the
appropriate compatibility checks and scans. Keep implementation details in code, contracts in schemas,
and verification results in build/release artifacts. Documentation should explain architecture,
operational responsibilities and decisions that remain useful as implementations evolve.
