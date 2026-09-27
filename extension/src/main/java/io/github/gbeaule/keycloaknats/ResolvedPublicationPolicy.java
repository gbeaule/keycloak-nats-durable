package io.github.gbeaule.keycloaknats;

/** Frozen decision and provenance. A null rule ID denotes the implicit indefinite retry policy. */
record ResolvedPublicationPolicy(PublicationPolicy policy, String filterSha256, String ruleId) {}
