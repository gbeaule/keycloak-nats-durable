"""Reject incomplete or stale shaded artifacts rather than trusting shade configuration."""

import contextlib
import io
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
import zipfile

import packaged_licenses


def copy_license_assets(root):
    source_root = Path(__file__).resolve().parent.parent
    for source, name, _ in packaged_licenses.license_assets(source_root):
        destination = root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())


def write_jar(root, module, artifact, version="1.2.3", changes=None):
    target = root / module / "target"
    target.mkdir(parents=True, exist_ok=True)
    resources = {resource: source.read_bytes()
                 for source, _, resource in packaged_licenses.license_assets(root)}
    bc = ("io/github/gbeaule/keycloaknats/internal/bouncycastle/" if module == "extension"
          else "org/bouncycastle/") + "LICENSE.class"
    resources[bc] = b"Copyright (c) Permission is hereby granted SOFTWARE IS PROVIDED"
    if module == "consumer-example":
        resources.update({
            "META-INF/LICENSE": b"PostgreSQL Global Development Group Grant of Patent License END OF TERMS AND CONDITIONS",
            "META-INF/LICENSE.txt": b"QOS.ch Permission is hereby granted",
            "META-INF/NOTICE": b"Jackson JSON processor FastDoubleParser Schubfach",
            "META-INF/FastDoubleParser-LICENSE": b"MIT License Werner Randelshofer",
            "META-INF/FastDoubleParser-ThirdParty-LICENSE": b"FastDoubleParser",
            "META-INF/Schubfach-LICENSE": b"Raffaello Giulietti Permission is hereby granted",
        })
        for component in ("scram-client", "scram-common", "saslprep", "stringprep"):
            resources[f"META-INF/licenses/com.ongres.example/{component}-1/META-INF/LICENSE"] = b"upstream notice"
    resources[f"META-INF/maven/io.github.gbeaule/{artifact}/pom.properties"] = f"version={version}\n".encode()
    resources.update(changes or {})
    jar = target / f"{artifact}-{version}.jar"
    with zipfile.ZipFile(jar, "w") as archive:
        for name, content in resources.items():
            if content is not None:
                archive.writestr(name, content)
    return jar


class PackagedLicensesTest(unittest.TestCase):
    def test_missing_or_truncated_license_and_lost_notice_collisions_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            copy_license_assets(root)
            for module, artifact in packaged_licenses.MODULES:
                jar = write_jar(root, module, artifact)
                packaged_licenses.check_jar(root, jar, module)
            for resource, content in (
                    ("META-INF/licenses/Apache-2.0.txt", None),
                    ("META-INF/licenses/Apache-2.0.txt", b"Apache License, Version 2.0"),
                    ("META-INF/licenses/project/LICENSE", None),
                    ("META-INF/LICENSE", b"PostgreSQL Global Development Group"),
                    ("META-INF/NOTICE", b"Jackson JSON processor"),
                    ("org/bouncycastle/LICENSE.class", None),
                    ("META-INF/Schubfach-LICENSE", None)):
                with self.subTest(resource=resource, content=content):
                    jar = write_jar(root, "consumer-example", "consumer-example", changes={resource: content})
                    with self.assertRaises((ValueError, KeyError)):
                        packaged_licenses.check_jar(root, jar, "consumer-example")

    def test_freshness_rejects_skipped_packaging_and_version_selection_ignores_old_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            copy_license_assets(root)
            jars = [write_jar(root, module, artifact) for module, artifact in packaged_licenses.MODULES]
            threshold = time.time_ns()
            for jar in jars:
                os.utime(jar, ns=(threshold - 10_000_000_000, threshold - 10_000_000_000))
            with patch.object(packaged_licenses, "__file__", str(root / "scripts/packaged_licenses.py")):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as failure:
                    packaged_licenses.main(["--built-after", str(threshold)])
                self.assertEqual(failure.exception.code, 1)
                for module, artifact in packaged_licenses.MODULES:
                    write_jar(root, module, artifact, version="2.3.4")
                with patch("builtins.print"):
                    self.assertEqual(packaged_licenses.main(["--built-after", str(threshold)]), 0)
                    self.assertEqual(packaged_licenses.main(["--version", "2.3.4"]), 0)
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    packaged_licenses.main([])


if __name__ == "__main__":
    unittest.main()
