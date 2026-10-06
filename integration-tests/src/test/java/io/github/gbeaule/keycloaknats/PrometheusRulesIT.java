package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.utility.MountableFile;

/** Checks real PromQL parsing and alert evaluation, including missing and stale metrics. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class PrometheusRulesIT {
  @Test
  void exampleAlertRulesHandleMissingTargetsAndSeries() {
    Path deploy = Path.of("..", "deploy").toAbsolutePath().normalize();
    try (var tool =
        new GenericContainer<>("prom/prometheus:v3.5.0")
            .withCreateContainerCmdModifier(command -> command.withEntrypoint("/bin/promtool"))
            .withCopyFileToContainer(
                MountableFile.forHostPath(deploy.resolve("prometheus-rules.yml")),
                "/rules/prometheus-rules.yml")
            .withCopyFileToContainer(
                MountableFile.forHostPath(deploy.resolve("prometheus-rules.test.yml")),
                "/rules/prometheus-rules.test.yml")
            .withWorkingDirectory("/rules")
            .withCommand("test", "rules", "prometheus-rules.test.yml")
            .withStartupCheckStrategy(new OneShotStartupCheckStrategy())
            .withStartupTimeout(Duration.ofSeconds(30))) {
      tool.start();
      assertTrue(tool.getLogs().contains("SUCCESS"), tool.getLogs());
    }
  }
}
