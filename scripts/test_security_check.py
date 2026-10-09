"""Require fresh build inputs and retain their identity with each security verdict."""

import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch
import zipfile

from test_release_tools import load
import validate


class SecurityBuildTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.module = load("security-check")
        self.module.__file__ = str(self.root / "scripts/security-check.py")
        self.version = "1.2.3"
        self.started = time.time_ns() - 10_000_000_000
        self.paths = []
        for module, artifact in self.module.MODULES:
            path = self.root / module / "target" / f"{artifact}-{self.version}.jar"
            path.parent.mkdir(parents=True)
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr(f"META-INF/maven/io.github.gbeaule/{artifact}/pom.properties",
                                 f"version={self.version}\n")
            self.paths.append(path)
        for module in ("", *(module for module, _ in self.module.MODULES)):
            target = self.root / module / "target"
            target.mkdir(parents=True, exist_ok=True)
            for name in ("bom.json", "runtime-bom.json"):
                path = target / name
                path.write_text(json.dumps({"bomFormat": "CycloneDX",
                    "metadata": {"component": {"version": self.version}},
                    "components": [{"name": "sample", "version": "1"}]}), encoding="utf-8")
                self.paths.append(path)
        for path in self.paths:
            os.utime(path, ns=(self.started + 1_000_000_000, self.started + 1_000_000_000))
        (self.root / "compose.yaml").write_text(
            "services:\n  postgres:\n    image: postgres:18.6-alpine\n"
            "  nats:\n    image: nats:2.15.0-alpine\n", encoding="utf-8")
        (self.root / "compose.producer.yaml").write_text(
            "services:\n  provision:\n    image: natsio/nats-box:0.19.7\n", encoding="utf-8")
        self.gate = self.root / "target/security/gate.json"
        self.gate.parent.mkdir()

    def scan(self, *command):
        if "--output" not in command:
            return
        destination = command[command.index("--output") + 1]
        if not destination.startswith("/reports/"):
            Path(destination).write_bytes(b"fixture image archive")
            return
        output = self.gate.parent / Path(destination).name
        output.write_text(json.dumps({"SchemaVersion": 2, "Results": [
            {"Class": "lang-pkgs", "Type": "jar", "Packages": [{"Name": "sample"}]}]}),
            encoding="utf-8")

    def arguments(self):
        return ["--version", self.version, "--built-after", str(self.started)]

    def assert_rejected_before_scanning(self, arguments=None):
        self.gate.write_text('{"blockingFindings": []}', encoding="utf-8")
        with patch.object(self.module, "run") as run, \
                contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as failure:
            self.module.main(self.arguments() if arguments is None else arguments)
        self.assertEqual(failure.exception.code, 2)
        run.assert_not_called()
        self.assertFalse(self.gate.exists())

    def test_build_timestamp_is_required_positive_and_not_in_the_future(self):
        for timestamp in (None, "0", "-1", str(time.time_ns() + 60_000_000_000)):
            with self.subTest(timestamp=timestamp):
                arguments = ["--version", self.version]
                if timestamp is not None:
                    arguments.extend(["--built-after", timestamp])
                self.assert_rejected_before_scanning(arguments)

    def test_every_inventory_and_packaged_jar_must_be_fresh_and_present(self):
        for path in self.paths:
            with self.subTest(path=path.relative_to(self.root)):
                content = path.read_bytes()
                os.utime(path, ns=(self.started - 1, self.started - 1))
                self.assert_rejected_before_scanning()
                path.unlink()
                self.assert_rejected_before_scanning()
                path.write_bytes(content)

    def test_malformed_empty_or_mismatched_inventories_cannot_pass(self):
        path = self.root / "target/runtime-bom.json"
        for inventory in ([], {}, {"bomFormat": "other", "components": [{}]},
                          {"bomFormat": "CycloneDX", "components": []},
                          {"bomFormat": "CycloneDX", "components": [None]},
                          {"bomFormat": "CycloneDX", "components": [{}], "metadata": None},
                          {"bomFormat": "CycloneDX", "components": [{}],
                           "metadata": {"component": {"version": "9.9.9"}}}):
            with self.subTest(inventory=inventory):
                path.write_text(json.dumps(inventory), encoding="utf-8")
                self.assert_rejected_before_scanning()
        path.write_text("{", encoding="utf-8")
        self.assert_rejected_before_scanning()

    def test_fresh_filename_cannot_hide_wrong_embedded_artifact_version(self):
        for path, (_, artifact) in zip(self.paths, self.module.MODULES):
            with self.subTest(artifact=artifact):
                original = path.read_bytes()
                with zipfile.ZipFile(path, "w") as archive:
                    archive.writestr(f"META-INF/maven/io.github.gbeaule/{artifact}/pom.properties",
                                     "version=9.9.9\n")
                self.assert_rejected_before_scanning()
                path.write_bytes(original)

    def test_fresh_build_records_exact_inventory_and_artifact_hashes(self):
        expected = {path.relative_to(self.root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in self.paths}
        with patch.object(self.module, "run", side_effect=self.scan), \
                contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as result:
            self.module.main(self.arguments())
        self.assertEqual(result.exception.code, 0)
        gate = json.loads(self.gate.read_text(encoding="utf-8"))
        self.assertEqual(gate["build"], {"version": self.version, "startedAtUnixNs": self.started,
                                       "sha256": expected})
        self.assertEqual(gate["blockingFindings"], [])
        self.assertEqual(list(self.gate.parent.glob("*.tar")), [])

    def test_inputs_changing_during_scanning_prevent_a_verdict(self):
        for path in (self.paths[0], self.root / "target/runtime-bom.json"):
            with self.subTest(path=path.relative_to(self.root)):
                original = path.read_bytes()
                def scan_and_change(*command):
                    self.scan(*command)
                    if "/reports/infrastructure-2.json" in command:
                        if path.suffix == ".jar":
                            with zipfile.ZipFile(path, "a") as archive:
                                archive.writestr("changed.txt", "new build")
                        else:
                            inventory = json.loads(original)
                            inventory["components"][0]["version"] = "2"
                            path.write_text(json.dumps(inventory), encoding="utf-8")
                self.gate.write_text('{"blockingFindings": []}', encoding="utf-8")
                with patch.object(self.module, "run", side_effect=scan_and_change), \
                        contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as failure:
                    self.module.main(self.arguments())
                self.assertEqual(failure.exception.code, 2)
                self.assertFalse(self.gate.exists())
                path.write_bytes(original)

    def test_successful_offline_build_cannot_reuse_previous_inventories(self):
        # Maven can succeed while CycloneDX skips offline generation. Keep its previous BOMs.
        threshold = time.time_ns()
        commands = []
        stderr = io.StringIO()
        def execute(command):
            commands.append(command)
            if command[0] == "mvn":
                self.assertIn("-o", command)
                for path in self.paths[:2]:
                    os.utime(path, ns=(threshold + 1_000_000_000, threshold + 1_000_000_000))
            elif "scripts/security-check.py" in command:
                try:
                    self.module.main(["--version", self.version, *command[2:]])
                except SystemExit as failure:
                    raise subprocess.CalledProcessError(failure.code, command) from failure
        self.gate.write_text('{"blockingFindings": []}', encoding="utf-8")
        with patch.object(validate, "ROOT", self.root), \
                patch.object(validate, "compatibility_matrix", return_value={}), \
                patch.object(validate.shutil, "which", return_value="mvn"), \
                patch.object(validate.time, "time_ns", return_value=threshold), \
                patch.object(validate, "run", side_effect=execute), \
                patch.object(self.module, "run") as docker, contextlib.redirect_stderr(stderr):
            self.assertEqual(validate.main(["security", "--maven-arg=-o"]), 2)
        docker.assert_not_called()
        self.assertFalse(self.gate.exists())
        self.assertIn("Missing or stale build input: target/bom.json", stderr.getvalue())
        self.assertEqual(commands[-1][-2:], ["--built-after", str(threshold)])


if __name__ == "__main__":
    unittest.main()
