package io.github.gbeaule.keycloaknats.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class SubjectPatternTest {
  @ParameterizedTest
  @CsvSource({
    "a.*,a.b,true",
    "a.*,a,false",
    "a.*,a.b.c,false",
    "a.>,a,false",
    "a.>,a.b,true",
    "a.>,a.b.c,true",
    ">,a,true",
    ">,a.b,true",
    "*,a,true",
    "*,a.b,false",
    "a.b,a,false",
    "a.b,a.b.c,false",
    "a.b,a.b,true",
    "a.b,a.c,false",
    "a.b,A.b,false",
    "a.b,a.*,false",
    "a.b,a.>,false",
    "keycloak.events.*.user.login,keycloak.events.realm.user.login,true",
    "keycloak.events.*.user.login,keycloak.events.realm.admin.user.update,false",
    "a.>,a..b,false"
  })
  void followsNatsTokenSemantics(String pattern, String subject, boolean expected) {
    assertEquals(expected, new SubjectPattern(pattern).matches(subject));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "a..b",
        ".a",
        "a.",
        "a.>.b",
        "a.b*",
        "a.**",
        "a.>>",
        "a. b",
        "a\nb",
        "a.\u0000b"
      })
  void rejectsMalformedPatterns(String value) {
    assertThrows(IllegalArgumentException.class, () -> new SubjectPattern(value));
  }

  @Test
  void patternLengthBoundsAreInclusive() {
    String maximum = "a".repeat(512);
    assertTrue(new SubjectPattern(maximum).matches(maximum));
    assertThrows(IllegalArgumentException.class, () -> new SubjectPattern(maximum + "a"));
    assertThrows(IllegalArgumentException.class, () -> new SubjectPattern(null));
  }

  @Test
  void absentOrEmptySubjectTokensNeverMatchEvenTheBroadestWildcard() {
    var pattern = new SubjectPattern(">");
    for (String subject : new String[] {null, "", ".a", "a.", "a..b"}) {
      assertFalse(pattern.matches(subject), "Invalid subject must not match: " + subject);
    }
  }
}
