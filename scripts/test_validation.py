"""Check local/CI matrix parity, safe inputs and propagation of failed validation."""

import contextlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import validate


class CompatibilityMatrixTest(unittest.TestCase):
    def test_candidate_is_added_once_without_changing_supported_versions(self):
        supported = validate.compatibility_matrix()
        self.assertEqual(validate.compatibility_matrix(supported["keycloak"][0]), supported)
        candidate = validate.compatibility_matrix("99.0.0")
        self.assertEqual(candidate["keycloak"], supported["keycloak"] + ["99.0.0"])
        self.assertEqual(validate.compatibility_matrix(), supported)

    def test_invalid_candidates_are_rejected_before_starting_a_build(self):
        for candidate in ("latest", "01.2.3", "v1.2.3", "١.2.3", "26.7.4 -DskipTests",
                          "26.7.4\nEVIL=1", "26.7.4; echo injected", "$(echo injected)"):
            with self.subTest(candidate=candidate), patch.object(validate, "run") as run:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    validate.main(["integration", "--keycloak-version", candidate])
                run.assert_not_called()

    def test_invalid_matrix_files_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config").mkdir()
            path = root / "config" / "keycloak-versions.json"
            with patch.object(validate, "ROOT", root):
                for content in ("{", "null", "{}", '"26.7.4"', "[]", '[null]', '[26]', '["latest"]'):
                    with self.subTest(content=content):
                        path.write_text(content, encoding="utf-8")
                        with self.assertRaises(ValueError):
                            validate.compatibility_matrix()

    def test_matrix_json_needs_no_maven_or_docker(self):
        output = io.StringIO()
        with patch.object(validate, "run") as run, patch.object(validate.shutil, "which") as which:
            with contextlib.redirect_stdout(output):
                self.assertEqual(validate.main(["matrix-json"]), 0)
            run.assert_not_called()
            which.assert_not_called()
        self.assertEqual(json.loads(output.getvalue()), validate.compatibility_matrix())


class ValidationRunnerTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "config").mkdir()
        (root / "config/keycloak-versions.json").write_bytes(
            (validate.ROOT / "config/keycloak-versions.json").read_bytes())
        workspace = patch.object(validate, "ROOT", root)
        workspace.start()
        self.addCleanup(workspace.stop)

    def test_postgres_selection_keeps_source_and_existing_durability_coverage(self):
        expected = {
            "ConsumerHardeningIT", "ConsumerStartupIT", "CustomSchemaIT", "UpgradeIT",
            "AdminUserAttributionIT", "OrganizationGroupAttributionIT", "ConsumerReconnectIT",
            "NatsReadinessIT", "PerUserCaptureIT",
            "PerUserRelayIT", "OutboxDiscardIT", "OutboxAuditRetentionIT",
            "NestedEventCaptureIT", "OutboxHeadIT", "OutboxSchemaIT",
            "PerUserPublicationIT", "PerUserSystemIT", "FilteringIT",
            "DurabilityIT#uncommittedAndRolledBackRowsAreNeverPublished+"
            "outboxWritesKeepSynchronousCommitForAuthenticationOnlyTransactions+"
            "retriesDoNotRewritePayloadAndVacuumSettingsAreInstalled",
        }
        for major in validate.POSTGRES_MAJORS:
            with self.subTest(postgres=major):
                command, = validate.maven_commands("postgres", {}, postgres_major=major)
                selection, = (arg.removeprefix("-Dit.test=") for arg in command
                              if arg.startswith("-Dit.test="))
                self.assertTrue(expected.issubset(selection.split(",")))

    def test_local_matrix_matches_individual_ci_selections_and_keeps_tests_enabled(self):
        matrix = validate.compatibility_matrix("99.0.0")
        expected = []
        for version in matrix["keycloak"]:
            expected.extend(validate.maven_commands("integration", matrix, keycloak_version=version))
        for major in matrix["postgres"]:
            expected.extend(validate.maven_commands("postgres", matrix, postgres_major=major))
        commands = validate.maven_commands("matrix", matrix)
        self.assertEqual(commands, expected)
        for command in commands:
            self.assertIn("-Pintegration", command)
            self.assertEqual(command[-1], "verify")
            self.assertFalse(any("skip" in argument.lower() for argument in command))

    def test_failed_python_checks_prevent_maven_and_security_scan(self):
        with patch.object(validate.shutil, "which", return_value="mvn"):
            with patch.object(validate, "run", side_effect=subprocess.CalledProcessError(7, "python")) as run:
                self.assertEqual(validate.main(["security"]), 7)
        self.assertEqual(run.call_count, 1)
        self.assertIn("unittest", run.call_args.args[0])

    def test_failed_build_prevents_security_scan_or_later_matrix_entries(self):
        for mode in ("security", "matrix"):
            with self.subTest(mode=mode), patch.object(validate.shutil, "which", return_value="mvn"):
                with patch.object(validate, "run", side_effect=[None, subprocess.CalledProcessError(9, "mvn")]) as run:
                    self.assertEqual(validate.main([mode]), 9)
                self.assertEqual(run.call_count, 2)

    def test_quick_checks_package_without_a_container_profile(self):
        with patch.object(validate.shutil, "which", return_value="mvn"):
            with patch.object(validate, "run") as run:
                self.assertEqual(validate.main([]), 0)
        self.assertEqual(run.call_count, 3)
        command = run.call_args_list[1].args[0]
        self.assertEqual(command[-1], "verify")
        self.assertFalse(any(argument.startswith("-P") or "skip" in argument.lower() for argument in command))
        self.assertIn("scripts/packaged_licenses.py", run.call_args.args[0])

    def test_failed_packaged_license_check_prevents_security_scan(self):
        with patch.object(validate.shutil, "which", return_value="mvn"):
            with patch.object(validate, "run", side_effect=[None, None,
                    subprocess.CalledProcessError(5, "packaged_licenses")]) as run:
                self.assertEqual(validate.main(["security"]), 5)
        self.assertEqual(run.call_count, 3)
        self.assertIn("--built-after", run.call_args.args[0])

    def test_security_scan_receives_timestamp_captured_before_maven(self):
        commands = []
        def execute(command):
            commands.append(command)
            if command[0] == "mvn":
                self.assertEqual(clock.call_count, 1)
        with patch.object(validate.shutil, "which", return_value="mvn"):
            with patch.object(validate.time, "time_ns", return_value=123456789) as clock:
                with patch.object(validate, "run", side_effect=execute):
                    self.assertEqual(validate.main(["security"]), 0)
        self.assertEqual(commands[-1][1:],
                         ["scripts/security-check.py", "--built-after", "123456789"])
        self.assertEqual(commands[-2][-2:], ["--built-after", "123456789"])

    def test_failed_security_validation_removes_previous_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gate = root / "target/security/gate.json"
            gate.parent.mkdir(parents=True)
            with patch.object(validate, "ROOT", root), \
                    patch.object(validate, "compatibility_matrix", return_value={}), \
                    patch.object(validate.shutil, "which", return_value="mvn"):
                for successful_steps in (0, 1, 2):
                    with self.subTest(successful_steps=successful_steps):
                        gate.write_text('{"blockingFindings": []}')
                        results = [None] * successful_steps + [subprocess.CalledProcessError(9, "validation")]
                        with patch.object(validate, "run", side_effect=results) as run:
                            self.assertEqual(validate.main(["security"]), 9)
                        self.assertFalse(gate.exists())
                        self.assertFalse(any("scripts/security-check.py" in call.args[0]
                                             for call in run.call_args_list))

    def test_runtime_flags_are_not_silently_ignored(self):
        for args in (["postgres"], ["quick", "--postgres-major", "14"],
                     ["security", "--keycloak-version", "26.7.4"]):
            with self.subTest(args=args), patch.object(validate, "run") as run:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    validate.main(args)
                run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
