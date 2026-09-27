# Local validation and Actions usage

Run fast checks locally before pushing. Java 21+, Maven 3.9+ and Python 3.9+ are required; set
`JAVA_HOME` to the JDK Maven should use. Use `python` instead of `python3` on Windows.

```sh
python3 scripts/validate.py                 # unit tests, style, packaging and Python tooling tests
python3 scripts/validate.py integration     # full container suite on the default Keycloak version
python3 scripts/validate.py matrix          # every Keycloak and older PostgreSQL CI selection
python3 scripts/validate.py security        # fresh SBOMs and dependency/image scans
```

The default `quick` mode needs no Docker daemon. Container testing and scans require a running Docker engine.
Integration tests fail when Docker is unavailable. The script stops on the first failed command and
returns its failure status. All commands run from the repository root, even when invoked elsewhere.
The matrix and individual CI jobs share the same version validation and Maven test selections.

For focused container testing or a proposed Keycloak release:

```sh
python3 scripts/validate.py integration --keycloak-version 26.6.4
python3 scripts/validate.py postgres --postgres-major 17
python3 scripts/validate.py matrix --keycloak-version 27.0.0
```

The last command adds a candidate to the local run without changing supported versions. Run the
container checks relevant to a code change before review; database, delivery, upgrade and dependency
changes should receive the full local matrix. Run `security` when dependencies or deployment images
change. Review changes to `.github/workflows/` with [actionlint](https://github.com/rhysd/actionlint).
Pass extra Maven settings as repeatable `--maven-arg=-Dname=value` arguments; for example,
`--maven-arg=-Dmaven.repo.local=.work/m2`. Security inventories require online Maven mode.

## When hosted jobs run

| Event | Checks |
| --- | --- |
| Feature branch push without a PR | None; validate locally |
| Pull request, including later pushes to its branch | One `quick-checks` job |
| Push/merge to `main` with changes beyond Markdown or `LICENSE` | Quick checks, then the full Keycloak/PostgreSQL matrix; separate security scan |
| Push to `main` changing only Markdown or `LICENSE` | None |
| Manual `verify` run | Quick checks and the full matrix, with an optional candidate Keycloak release |
| Monday 08:17 UTC, or manual `security` run | Fresh dependency and image security scan |
| `v*` tag, or manual `release-candidate` run | Fresh release build, integration suite, inventories, scans and checksummed bundle |

PR workflows deliberately have no path filter: required checks must report a result even for a
documentation change. JSON examples under `docs/examples/` are executable test fixtures and still
trigger post-merge verification. Manual runs can validate a branch before merging when needed.
Security scans remain weekly because vulnerability findings can change without a source change.

Before this change, a commit pushed to a branch with an open PR could start **16 jobs**: the push and
PR each ran a setup job, two Keycloak jobs, four PostgreSQL jobs and one security job. It now starts **one**
quick job. This is a job-count comparison, not a measured billing percentage. Full compatibility
coverage remains on `main`, with manual/local runs available before merging. Failures in that
coverage are therefore normally discovered after merge and must be addressed before a release.

Superseded verification runs are cancelled within the same event and branch/PR; manual candidate
runs have separate concurrency groups. Security runs share one group per branch so a push, schedule
and manual request cannot run duplicate scans concurrently. Matrix jobs start only after quick checks pass and
stop sibling entries on failure. Every Java job caches Maven dependencies. Routine verification
retains reports/logs for three days and uploads no JARs; quick checks upload reports only on failure.
Security reports are kept for seven days. Release evidence and bundles retain the existing 90 days.
These retention changes reduce artifact storage, separately from runner-minute savings.

Dependabot batches weekly minor/patch updates per ecosystem and limits each ecosystem to three open
version-update PRs. Major upgrades remain separate for review; security updates are not delayed by
these version-update groups. Fewer routine update PRs also mean fewer post-merge matrices.

## Repository settings and rollout

Use **`quick-checks` from `verify`** as the required PR status check. If existing branch protection or
rulesets require `versions`, `test (...)`, `postgres-compatibility (...)` or `dependencies-and-images`,
replace those requirements when adopting this workflow. Matrix and security checks are post-merge
checks and must not be required to merge a PR. Repository settings are not changed by these files.
See [GitHub's required-check behavior](https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/troubleshooting-required-status-checks).

Workflow savings take effect after the changes reach GitHub; existing runs keep their original job
definitions. No extra Actions runs are needed to validate this change locally. Inspect account
Settings → Billing and licensing → Usage, filtered to Actions and this repository, to compare billed
minutes after rollout. The account allowance is shared across private repositories, so optimizing
this repository alone may not account for the whole email. See
[GitHub Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions).
Keep the existing budget/spending limit rather than raising it to compensate for duplicate builds.
