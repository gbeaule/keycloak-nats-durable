# Pull request CI security

The `pull_request` workflow intentionally executes the proposed Java code, tests, Maven plugins and build scripts. A contributor can change any of those to execute arbitrary commands. The runner belongs to a workflow run in **this base repository**, not to the contributor's fork. GitHub supplies a fresh hosted Ubuntu VM for each job; the tests also use its Docker daemon. This is an isolation boundary, not a promise that PR code is harmless.

For fork PRs, GitHub normally withholds repository secrets and restricts `GITHUB_TOKEN` to read access. This workflow requests only `contents: read`, does not reference secrets or deployment environments, disables checkout credential persistence, pins actions to full commit IDs, and uses hosted runners. It has no publish/deploy step. See [GitHub's fork PR behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull_request) and [security guidance](https://docs.github.com/en/actions/reference/security/secure-use).

Repository administrators must keep fork write-token/secret sharing disabled and configure approval for outside contributors under Settings → Actions → General. Those settings cannot be enforced by this YAML, and a contributor can propose changes to the YAML itself. Approval should review the current changes, including workflow files, POMs and tests. Same-repository PRs come from writers and are not subject to all fork restrictions; review and branch protection still matter. Require review of workflow changes and never place production credentials in this test environment.

Do not move this job to a self-hosted runner connected to private services. Do not change it to `pull_request_target` and then check out PR code. Logs, JARs, test reports and caches produced by a PR are untrusted: never promote them into a privileged release job. Build releases again from reviewed, trusted commits. Maven caches contain public build dependencies only; do not cache credentials or confidential files. GitHub scopes PR caches to the PR merge ref; see [cache access restrictions](https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows#restrictions-for-accessing-a-cache).

Running an untrusted build still allows outbound network traffic, consumption of CI minutes and reading anything supplied to that runner. The timeout and cancellation settings limit ordinary resource use but do not prevent malicious computation. These restrictions are appropriate for public test builds with disposable infrastructure; they are not an unrestricted security guarantee.

Manual candidate-version testing uses the same isolated jobs and permissions. Its input is read as data through an environment variable, validated as an explicit `major.minor.patch` release, then passed to Maven as one quoted argument. It is never interpolated into shell source. The separate matrix job also uses a hosted VM, read-only permissions and checkout with credential persistence disabled. This input check prevents accidental argument/script injection; it does not make PR-provided build code trusted.

The security and release-candidate workflows use the same hosted-runner and read-only permission
boundary. Security scanning builds proposed Dockerfiles as untrusted code, with no publishing
credentials. The candidate workflow rebuilds source and packages hashes/SBOMs; it does not consume PR
artifacts or publish a release. See [release and patching procedures](releases.md). Administrator-owned
branch/tag protections and required status checks remain necessary outside these workflow files.
