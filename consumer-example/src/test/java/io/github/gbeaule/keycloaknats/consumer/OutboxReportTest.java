package io.github.gbeaule.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class OutboxReportTest {
  @Test
  void reportFilterTreatsSqlAndRegexMetacharactersAsLiteralSubjectContent() {
    var regex = Pattern.compile(OutboxReport.sqlPattern("events.tenant[1].*"));
    assertTrue(regex.matcher("events.tenant[1].login").matches());
    assertFalse(regex.matcher("events.tenant1.login").matches());
    assertFalse(regex.matcher("events.tenant[1].admin.login").matches());
    var terminal = Pattern.compile(OutboxReport.sqlPattern("events.>"));
    assertTrue(terminal.matcher("events.a.login").matches());
    assertFalse(terminal.matcher("events").matches());
  }
}
