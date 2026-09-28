"""Regression checks for release input validation and the owner-accepted upstream scan policy."""

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReleaseIdentityTest(unittest.TestCase):
    def test_tag_overrides_dispatch_version_and_exports_commit_timestamp(self):
        module = load("release-identity")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "environment"
            with patch.dict(os.environ, {"REQUESTED_VERSION": "9.9.9", "RELEASE_REF_TYPE": "tag",
                                         "RELEASE_REF": "v1.2.3", "GITHUB_ENV": str(output)}):
                with patch.object(module.subprocess, "check_output", return_value="1234567890\n"):
                    module.main()
            self.assertEqual(output.read_text(), "RELEASE_VERSION=1.2.3\nSOURCE_DATE_EPOCH=1234567890\n")

    def test_dispatch_uses_requested_version_and_rejects_environment_injection(self):
        module = load("release-identity")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "environment"
            with patch.dict(os.environ, {"RELEASE_REF_TYPE": "branch", "GITHUB_ENV": str(output)}):
                with patch.object(module.subprocess, "check_output", return_value="123\n") as git:
                    for invalid in ("", "01.2.3", "1.2.3-SNAPSHOT", "v1.2.3", "1.2.3\nEVIL=1",
                                    "1.2.3; touch nope", "١.2.3"):
                        with self.subTest(version=invalid):
                            with patch.dict(os.environ, {"REQUESTED_VERSION": invalid}):
                                with self.assertRaises(SystemExit):
                                    module.main()
                    git.assert_not_called()
                    self.assertFalse(output.exists())
                    with patch.dict(os.environ, {"REQUESTED_VERSION": "2.3.4"}):
                        module.main()
                    self.assertEqual(output.read_text(), "RELEASE_VERSION=2.3.4\nSOURCE_DATE_EPOCH=123\n")


class SecurityPolicyTest(unittest.TestCase):
    def test_shipped_fixes_block_while_upstream_findings_remain_visible(self):
        module = load("security-check")
        vulnerability = {"VulnerabilityID": "CVE-test", "PkgName": "example", "InstalledVersion": "1",
                         "FixedVersion": "2", "Severity": "HIGH"}
        with tempfile.TemporaryDirectory() as directory:
            reports = []
            for name in ("runtime-dependencies.json", "dependencies.json", "keycloak.json",
                         "consumer.json", "infrastructure-0.json", "infrastructure-1.json"):
                path = Path(directory) / name
                path.write_text(json.dumps({"SchemaVersion": 2,
                                            "Results": [{"Class": "lang-pkgs", "Type": "jar",
                                                         "Packages": [{"Name": "example"}],
                                                         "Vulnerabilities": [vulnerability]}]}))
                reports.append(path)
            gate = module.evaluate(reports)
            self.assertEqual(len(gate["blockingFindings"]), 1)
            self.assertEqual(len(gate["advisoryFindings"]), 5)
            self.assertTrue(all(f["severity"] == "HIGH" for f in gate["advisoryFindings"]))
            reports[0].write_text(json.dumps({"SchemaVersion": 2, "Results": [
                {"Class": "lang-pkgs", "Type": "jar", "Packages": [{"Name": "example"}]}]}))
            self.assertEqual(module.evaluate(reports)["blockingFindings"], [])

    def test_missing_or_incomplete_runtime_scan_cannot_pass(self):
        module = load("security-check")
        with self.assertRaises(ValueError):
            module.evaluate([])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime-dependencies.json"
            for report in ({}, {"SchemaVersion": 2, "Results": []},
                           {"SchemaVersion": 2, "Results": [{}]},
                           {"SchemaVersion": 2, "Results": [
                               {"Class": "lang-pkgs", "Type": "jar", "Packages": []}]},
                           {"SchemaVersion": 2, "Results": [
                               {"Class": "os-pkgs", "Type": "alpine", "Packages": [{"Name": "example"}]}]}):
                with self.subTest(report=report):
                    path.write_text(json.dumps(report))
                    with self.assertRaises(ValueError):
                        module.evaluate([path])


class ReleaseManifestTest(unittest.TestCase):
    def test_version_tags_and_both_inventory_kinds_are_preserved(self):
        module = load("release-manifest")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            module.__file__ = str(root / "scripts" / "release-manifest.py")
            for name, artifact in (("extension", "keycloak-nats-durable"),
                                   ("consumer-example", "consumer-example")):
                target = root / name / "target"
                target.mkdir(parents=True)
                with zipfile.ZipFile(target / f"{artifact}-1.2.3.jar", "w") as jar:
                    jar.writestr(f"META-INF/maven/io.github.gbeaule/{artifact}/pom.properties",
                                 "version=1.2.3\n")
            for name in ("", "extension", "consumer-example"):
                target = root / name / "target"
                target.mkdir(parents=True, exist_ok=True)
                for filename in ("bom.json", "runtime-bom.json"):
                    (target / filename).write_text(json.dumps({"bomFormat": "CycloneDX",
                        "metadata": {"component": {"version": "1.2.3"}},
                        "components": [{"name": "sample", "version": "1"}]}))
            (root / "compose.yaml").write_text("services:\n  postgres:\n    image: postgres:18.6-alpine\n"
                                                "  nats:\n    image: nats:2.15.0-alpine\n")
            (root / "compose.producer.yaml").write_text(
                "services:\n  postgres:\n    image: postgres:18.6-alpine\n"
                "  provision:\n    image: natsio/nats-box:0.19.7\n")
            (root / "deploy").mkdir()
            (root / "deploy" / "Dockerfile.keycloak").write_text(
                "ARG KEYCLOAK_IMAGE=quay.io/keycloak/keycloak:26.7.4\n"
                "FROM ${KEYCLOAK_IMAGE}\n")
            (root / "deploy" / "Dockerfile.consumer").write_text("FROM eclipse-temurin:21-jre\n")
            output = root / "target" / "release"
            with patch("sys.argv", ["release-manifest", "--version", "1.2.3", "--commit", "b" * 40,
                                    "--output", str(output)]):
                with patch("builtins.print"):
                    module.main()
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest["baseImages"]["compose.yaml"],
                             ["postgres:18.6-alpine", "nats:2.15.0-alpine"])
            self.assertEqual(manifest["baseImages"]["compose.producer.yaml"],
                             ["postgres:18.6-alpine", "natsio/nats-box:0.19.7"])
            self.assertEqual(manifest["baseImages"]["deploy/Dockerfile.keycloak"],
                             ["quay.io/keycloak/keycloak:26.7.4"])
            self.assertEqual(manifest["baseImages"]["deploy/Dockerfile.consumer"],
                             ["eclipse-temurin:21-jre"])
            for name in ("aggregate", "extension", "consumer-example"):
                self.assertIn(name + "-bom.json", manifest["artifacts"])
                self.assertIn(name + "-runtime-bom.json", manifest["artifacts"])


if __name__ == "__main__":
    unittest.main()
