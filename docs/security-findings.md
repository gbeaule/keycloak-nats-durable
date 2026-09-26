# Security baseline assessment, 2026-09-26

**Owner decision:** accept upstream image findings as advisory and consume Keycloak and PostgreSQL
as released. These findings do not block this project's release candidate. We do not replace
Keycloak's internal JARs or rebuild PostgreSQL's bundled utilities.

Trivy 0.74.0 scans the complete and shipped-runtime CycloneDX inventories and all four deployment
images. The gate still blocks HIGH/CRITICAL findings with a published fix in dependencies shipped by
this project. Provided Keycloak libraries are excluded from that runtime inventory, while remaining
visible in the complete inventory and image scan. Full reports retain all severities in
`target/security`; acceptance does not relabel scanner findings as low severity or prove they are
unreachable. Scanner/network failures continue to fail the job.

| Surface | Result requiring attention |
|---|---|
| Consumer application image | No fixed HIGH/CRITICAL finding in the recorded scan. |
| NATS 2.15.0 Alpine image | No fixed HIGH/CRITICAL finding in the recorded scan. Updating from 2.12.8 removed the old Go/crypto and Alpine findings. |
| Keycloak 26.7.4 image | Owner-accepted findings for Netty 4.1.136.Final, Bouncy Castle 1.84 and FreeMarker 2.3.32; the SQL Server JDBC version finding may be a normalization issue. |
| PostgreSQL 18.6 Alpine image | Findings are in the Go runtime linked into `/usr/local/bin/gosu`, not the PostgreSQL server or Alpine packages. |
| Aggregate dependency inventory | Netty is a provided Keycloak dependency; it is not bundled into this provider JAR. The runtime image scan confirms it is present in Keycloak. |

The official [Keycloak downloads page](https://www.keycloak.org/downloads) still identifies 26.7.4 as
the latest stable release at this assessment. Replacing Keycloak's internal JARs inside a provider
would not establish a supported or validated server patch. Retain the findings for visibility, track
upstream releases, and rerun compatibility tests and scans when choosing an upstream update.

Keycloak findings reported by the scanner:

- `CVE-2026-75595`: Netty SNI routing fallback; patched versions include 4.1.137.Final. The
  [upstream advisory](https://github.com/netty/netty/security/advisories/GHSA-c4c3-7fpv-j4q5) describes
  the conditions under which permissive fallback TLS configuration can bypass per-SNI mTLS.
- `CVE-2026-8763` and `CVE-2026-13506`: Bouncy Castle certificate-name constraints and ASN.1 handling;
  the scanner identifies 1.85 as the fixed version for the Keycloak-bundled 1.84 library. These are
  distinct from this extension's relocated Bouncy Castle LTS dependency.
- `CVE-2026-84939`: FreeMarker locale path traversal; the scanner identifies 2.3.35 as the fix.
- `CVE-2025-59250`: SQL Server JDBC is reported as `13.2.1`, while the advisory uses the `13.2.1.jre11`
  artifact version. This may be a version-normalization issue. The example deployment uses PostgreSQL;
  the scanner record is retained as an advisory finding.

The PostgreSQL image's `gosu` binary carries Go 1.24.6, which produces numerous standard-library
findings. Many advisories concern network protocols that a privilege-switching command may never use;
binary inventory alone does not establish reachability. These owner-accepted findings remain in
reports while the project uses the upstream image unchanged.

There is no vulnerability ignore list. Acceptance changes the repository's gate classification, not
the scanner output. The original scan used a stricter policy and failed; that historical result is
preserved in the verification evidence. Current gate results distinguish `blockingFindings` from
`advisoryFindings`. See [release and update procedures](releases.md).

The scan completed at **2026-09-26 19:32:52 UTC** with **0 blocking records and 30 advisory records**
under this policy. Records can repeat an advisory across inventory/image surfaces; this is not a
count of distinct vulnerabilities. Evidence is in `.work/review-comments-evidence/security/`.
