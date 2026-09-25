# Java style

The standard is [Google Java Style](https://google.github.io/styleguide/javaguide.html), with the already agreed mandatory braces. Spotless 3.0.0 runs Google Java Format 1.27.0. Maven Checkstyle 3.6.0 uses Checkstyle 10.26.1's bundled `google_checks.xml`, including brace, import, naming, whitespace and public API documentation checks. Versions are pinned in the root POM. Google checks emit warnings by default; `violationSeverity=warning` makes those warnings fail the build.

```sh
mvn spotless:apply       # apply formatting and remove unused imports
mvn verify              # enforce formatting, Google Style, and unit tests
```

Use braces for `if`, `else`, `for`, `while` and `do`, including single statements. Use explicit imports, one declaration per statement, descriptive names for nontrivial conditions and named upstream constants for API sentinels. The standard uses UTF-8, LF, two-space indentation, K&R braces and a 100-column limit with its documented exceptions. Format long SQL as readable text blocks. Formatting does not insert missing braces or invent Javadoc; Checkstyle reports those for correction.

Uppercase constant names are reserved for deeply immutable values. Loggers, JSON mappers and HTTP clients use descriptive camelCase names even when their references are `static final`.

The only project naming exception is Maven Failsafe's conventional `IT` class suffix, documented by a local `AbbreviationAsWordInName` suppression on integration-test classes. Other style rules still apply there. Do not add broad style suppressions to avoid cleaning up code.

In IntelliJ, import the root Maven project with Java 21, keep EditorConfig support enabled, and run `spotless:apply` from the Maven tool window before committing. `.editorconfig` sets UTF-8/LF, two-space indentation, the 100-column margin, mandatory control-flow braces and explicit imports. Maven is the authoritative formatter/checker, so IDE and CI use the same pinned implementation. IntelliJ's default Java formatter can differ from Google Java Format.

Hibernate locking uses `SpecHints.HINT_SPEC_LOCK_TIMEOUT` and `Timeouts.SKIP_LOCKED_MILLI`, keeping provider-specific values named and centralized in `OutboxRepository`. A negative sentinel in another API, such as unlimited NATS reconnects, has a separate meaning and must not borrow a Hibernate constant.
