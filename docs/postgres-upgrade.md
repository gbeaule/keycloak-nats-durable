# PostgreSQL compatibility and optional major upgrades

The demo and full test suite default to PostgreSQL **18.6**. Existing installations can retain a
supported PostgreSQL **14–18** major compatible with their Keycloak release; there is no requirement
to upgrade to 18 to install this provider. See [compatibility coverage](compatibility.md).
Compose and Maven tests use the version tag `postgres:18.6-alpine`; the test image
can be overridden with `-Dpostgres.image=postgres:17-alpine`. Upstream supported versions
and patch policy are listed in the
[PostgreSQL version policy](https://www.postgresql.org/support/versioning/).

For a fresh demo on 14–17, select that major's image and mount its data volume at
`/var/lib/postgresql/data`; the 18 image uses the parent mount described below. Keep different majors
on distinct volumes. Merely pointing an existing volume at another major is never an upgrade or
downgrade procedure.

PostgreSQL 17 data cannot be opened directly by PostgreSQL 18. Use logical dump/restore or a rehearsed
`pg_upgrade` procedure with both major versions available. The official 18 image places data under
`/var/lib/postgresql/18/docker`; Compose now mounts the parent `/var/lib/postgresql`. The pinned
[image entrypoint](https://github.com/docker-library/postgres/blob/e00e1bd34ec5c8a8e7ad89b273b3d42efaf6d5bc/docker-entrypoint.sh)
detects older data directories and refuses initialization instead of replacing their data.

For an existing demo or production installation **choosing to upgrade to 18**:

1. Stop new application writes and consumer processing for a coordinated cutover. Record outbox,
   broker and inbox/effect counts and retain the current application artifacts and configuration.
2. Take verified backups of **both** Keycloak and consumer databases, including globals/roles and all
   provider tables. Retain JetStream's durable state and your application's recovery checkpoints.
   Store backups with the same access controls as the live data.
3. Restore into a **new** PostgreSQL 18 volume/cluster using PostgreSQL 18 client tools, or follow your
   database platform's tested major-upgrade procedure. Preserve the old volume and server binaries
   until the new cluster passes acceptance. Do not run `docker compose down -v` as an upgrade step.
4. Apply consumer migrations and let the supported Keycloak migration process validate its schema.
   Check database roles, TLS, durable commit settings, extensions, vacuum and replication settings.
5. Verify pending event IDs/hashes, inbox/effect counts, login/admin operations and backlog drain.
   Start consumers and confirm a duplicate delivery does not repeat an effect. Reopen traffic after
   those checks and retain the cutover evidence.

Keep the old and new cluster endpoints explicit in your migration commands; this repository does not
automatically connect to or overwrite an existing installation. A fresh Compose project can initialize
18 directly. Reusing an old 17 volume without migration deliberately fails startup. If remaining on 17,
use a supported current 17.x security patch and rehearse the 18 upgrade separately.

Follow the database platform's [major upgrade procedure](https://www.postgresql.org/docs/18/upgrading.html)
for the exact commands and extension compatibility checks. Restoring a consumer database alone to a
point before an already acknowledged WorkQueue event can erase its effect and inbox with no broker
copy left. Backups across the pipeline need a coordinated recovery point or an independent replay
archive; an image upgrade does not solve that recovery boundary.
