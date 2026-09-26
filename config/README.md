# Configuration examples

`events-all.json`, `events-disabled-only.json` and `events-scoped.json` are optional example capture
policies. They stay in the source repository; neither JAR, image nor candidate bundle contains them.
No example is loaded automatically. In particular, replace `replace-with-realm-id` in the scoped
example before using it.

To use a policy, copy it to a deployment-owned directory, mount that directory read-only into Keycloak
and point `KND_FILTER_FILE` to the file. Leaving `KND_FILTER_FILE` unset captures all events delivered
to the listener. See [capture configuration](../docs/configuration.md).

`keycloak-versions.json` is CI compatibility metadata, not runtime configuration.
