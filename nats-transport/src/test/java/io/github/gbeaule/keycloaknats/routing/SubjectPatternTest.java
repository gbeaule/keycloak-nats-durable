package io.github.gbeaule.keycloaknats.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class SubjectPatternTest {
  @ParameterizedTest
  @CsvSource({
    "a.*,a.b,true",
    "a.*,a.b.c,false",
    "a.>,a,false",
    "a.>,a.b,true",
    "a.>,a.b.c,true",
    ">,a,true",
    "a.b,a.b,true",
    "a.b,a.c,false",
    "keycloak.events.*.user.login,keycloak.events.realm.user.login,true",
    "keycloak.events.*.user.login,keycloak.events.realm.admin.user.update,false",
    "a.>,a..b,false"
  })
  void followsNatsTokenSemantics(String pattern, String subject, boolean expected) {
    assertEquals(expected, new SubjectPattern(pattern).matches(subject));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "a..b", ".a", "a.", "a.>.b", "a.b*", "a.**", "a.>>", "a. b", "a\nb"})
  void rejectsMalformedPatterns(String value) {
    assertThrows(IllegalArgumentException.class, () -> new SubjectPattern(value));
  }
}
