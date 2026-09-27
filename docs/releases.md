# Reproducible candidates, inventories and patching

Development builds keep the default `1.0.0-SNAPSHOT` version. The Maven reactor uses the CI-friendly
`revision` property so a candidate can have an explicit version without changing tracked POM files:

```sh
mvn -B -ntp -Pintegration,sbom -Drevision=1.0.0 \
  -Dproject.build.outputTimestamp=YOUR_COMMIT_EPOCH verify
python3 scripts/security-check.py --version 1.0.0
python3 scripts/release-manifest.py --version 1.0.0 --commit YOUR_FULL_COMMIT_SHA
```

Use the timestamp of the reviewed source commit, the recorded Java/Maven toolchain, and the checked-in
dependency versions. The `sbom` profile generates aggregate and per-module CycloneDX JSON inventories.
It includes provided dependencies to identify the Keycloak compatibility surface and excludes test
dependencies. A second `runtime-bom.json` inventory excludes provided dependencies and identifies
what this project ships; it drives the dependency gate. Provided libraries are not thereby bundled
into the provider. Inventory and image
scanning complement each other; relocated JAR classes alone are not a complete dependency inventory.
Use a fresh checkout/build and Maven online mode: CycloneDX skips its goal under `-o`, even when
dependencies are cached. Do not reuse an earlier build's inventory for a changed dependency graph.

The release manifest command verifies the embedded JAR versions, requires nonempty SBOMs, copies the
provider and consumer artifacts into `target/release`, records their SHA-256 hashes and source commit,
and includes deployment image references. Compose and Dockerfile references use version tags.
A tag can resolve to different bytes over time, so retain
the scan report's resolved image identity with the bundle. The manifest does not describe a tag as
immutable. It refuses to overwrite an existing bundle.
The fixed default build timestamp makes development packaging repeatable; release jobs override it
with the source commit timestamp. Preserve the manifest, checksum file, inventories and test evidence
with every deployed release. A checksum identifies bytes; it does not by itself prove who built them.
The JAR plugin always recreates its input before shading, so an incremental package does not reprocess
a prior shaded output. Fresh and repeated packaging are checked for identical JAR hashes.

The [release-candidate workflow](../.github/workflows/release-candidate.yml) builds fresh artifacts on
hosted runners from a tag or explicit version, runs integration tests and the security gate, and retains the checksummed
bundle and evidence for 90 days. It has read-only repository permissions and does not publish to a
registry, Maven Central or GitHub Releases. Administrators own reviewed tags, branch protection,
release approval, signing/provenance and long-term artifact storage. Never promote an untrusted pull
request artifact into production. Record the supported Keycloak runtime matrix results for the same
source commit before publication.

Container builds accept `ARTIFACT_VERSION` and `VCS_REF` build arguments. Both provider and consumer
bases use version tags, and the Keycloak image is built with health and metrics enabled for
`start --optimized`. The provider mtime is fixed before that build so container timestamp rounding
does not spuriously invalidate optimization. Pass the tested hostname/TLS/runtime configuration when
starting a production image; the Compose stack remains a development example.

## Security checks and updates

The [security workflow](../.github/workflows/security.yml) runs on pushes, pull requests, weekly and on
demand. It creates SBOMs and uses the `aquasec/trivy:0.74.0` image to scan dependencies, the built
provider/consumer container images and the versioned PostgreSQL/NATS images. Run the same checks locally:

```sh
mvn -B -ntp -Psbom verify
python3 scripts/security-check.py
```

Docker, Java 21, Maven and Python 3 are required. The script builds local images and downloads current
vulnerability databases, without publishing images or mounting the Docker socket into the scanner.
Reports under `target/security` retain all severities. The gate fails HIGH/CRITICAL findings with a
published fix in the **shipped runtime dependency inventory**. The owner accepts upstream image and
provided Keycloak dependency findings as advisory. This project consumes Keycloak and PostgreSQL as
released; it does not replace their internal libraries or rebuild their utilities. Accepted findings
remain visible with their original severities in scan reports and `gate.json`. A scanner/network
failure still fails the job rather than reporting a clean result. Vulnerability
results change as advisories and fixes change, even with identical application bytes.

Use version tags for dependency references throughout the repository: full release tags for GitHub
Actions and version/flavor tags for Dockerfiles, Compose, test containers and the scanner. Maven
dependency and plugin versions belong in the POMs.

[Dependabot configuration](../.github/dependabot.yml) requests weekly Maven, GitHub Actions, Dockerfile
and Compose updates. Changes are proposals, with no automatic merge. Review version-tag updates,
keep the default PostgreSQL/NATS test versions aligned with Compose, and rerun the supported runtime
matrix after changes. Update the scanner's version tag deliberately. Repository administrators must
enable the applicable GitHub security/update features and
require the verification and security jobs through branch protection; source files alone cannot set
those repository policies.

For PostgreSQL major versions, use the [migration runbook](postgres-upgrade.md). For Keycloak, keep
version-specific SPI compatibility and upgrade tests. Patch releases still require regression tests;
a successful functional build does not substitute for reviewing a security report.

The [2026-09-26 baseline assessment](security-findings.md) records outstanding upstream image findings.
Those owner-accepted upstream findings do not block candidate packaging. Updating a supported upstream
release is the patch path; approval of the final deployment remains with its owner.
