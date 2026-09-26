package io.github.gbeaule.keycloaknats.routing;

import java.util.Arrays;

/** Validated NATS subject filter: star matches one token, terminal greater-than one or more. */
public final class SubjectPattern {
  private final String[] tokens;

  /** Rejects empty tokens, partial wildcards, whitespace and nonterminal greater-than tokens. */
  public SubjectPattern(String pattern) {
    if (pattern == null || pattern.isEmpty() || pattern.length() > 512) {
      throw new IllegalArgumentException("Subject filter must contain 1..512 characters");
    }
    tokens = pattern.split("\\.", -1);
    for (int i = 0; i < tokens.length; i++) {
      String token = tokens[i];
      if (token.isEmpty()
          || token.chars().anyMatch(c -> Character.isWhitespace(c) || Character.isISOControl(c))
          || (token.contains("*") && !token.equals("*"))
          || (token.contains(">") && (!token.equals(">") || i != tokens.length - 1))) {
        throw new IllegalArgumentException("Invalid NATS subject filter");
      }
    }
  }

  /** Matches a concrete subject without treating its contents as a pattern. */
  public boolean matches(String subject) {
    if (subject == null || subject.isEmpty()) {
      return false;
    }
    String[] parts = subject.split("\\.", -1);
    if (Arrays.stream(parts).anyMatch(String::isEmpty)) {
      return false;
    }
    for (int i = 0; i < tokens.length; i++) {
      if (i >= parts.length) {
        return false;
      }
      if (tokens[i].equals(">")) {
        return true;
      }
      if (!tokens[i].equals("*") && !tokens[i].equals(parts[i])) {
        return false;
      }
    }
    return tokens.length == parts.length;
  }
}
