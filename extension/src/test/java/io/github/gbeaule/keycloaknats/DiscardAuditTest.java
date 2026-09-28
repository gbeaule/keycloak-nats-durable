package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DiscardAuditTest {
  @Test
  void auditCopiesOnlyAllowedMetadataIncludingUncertainPublication() throws Exception {
    var policy = new ResolvedPublicationPolicy(new PublicationPolicy(10, 2), "digest", "rule");
    var row =
        new OutboxEvent(
            "id",
            "subject",
            "private payload",
            1000,
            "realm",
            "type",
            new EventOrdering("realm", "user", 11),
            policy);
    row.markPublicationIntent();
    row.failed(2000, "private exception");
    var audit = new DiscardAudit(row, DiscardReason.EXPIRED, 11000);
    var metadata = new HashMap<String, Object>();
    for (var field : DiscardAudit.class.getDeclaredFields()) {
      field.setAccessible(true);
      metadata.put(field.getName(), field.get(audit));
    }
    assertEquals(
        Map.ofEntries(
            Map.entry("id", "id"),
            Map.entry("realmId", "realm"),
            Map.entry("eventType", "type"),
            Map.entry("subject", "subject"),
            Map.entry("payloadSha256", row.payloadSha256()),
            Map.entry("orderingKey", row.orderingKey()),
            Map.entry("userSequence", 11L),
            Map.entry("maxAgeSeconds", 10),
            Map.entry("maxFailures", 2),
            Map.entry("expiresAt", 11000L),
            Map.entry("filterSha256", "digest"),
            Map.entry("ruleId", "rule"),
            Map.entry("createdAt", 1000L),
            Map.entry("discardedAt", 11000L),
            Map.entry("reason", DiscardReason.EXPIRED),
            Map.entry("attempts", 1L),
            Map.entry("publicationMayHaveOccurred", true)),
        metadata);
    assertNotNull(new DiscardAudit());
  }
}
