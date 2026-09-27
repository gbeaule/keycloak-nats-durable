#!/usr/bin/env python3
"""Run the same validation locally and in CI; quick checks need no Docker daemon."""

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
POSTGRES_MAJORS = ("14", "15", "16", "17")
POSTGRES_TESTS = (
    "ConsumerHardeningIT,CustomSchemaIT,UpgradeIT,DurabilityIT#"
    "uncommittedAndRolledBackRowsAreNeverPublished+"
    "outboxWritesKeepSynchronousCommitForAuthenticationOnlyTransactions+"
    "retriesDoNotRewritePayloadAndVacuumSettingsAreInstalled"
)
RELEASE = re.compile(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)")


def compatibility_matrix(candidate=""):
    versions = json.loads((ROOT / "config" / "keycloak-versions.json").read_text(encoding="utf-8"))
    if not isinstance(versions, list) or not versions:
        raise ValueError("Keycloak versions must be a nonempty list")
    if candidate:
        versions.append(candidate)
    if any(not isinstance(version, str) or not RELEASE.fullmatch(version) for version in versions):
        raise ValueError("Keycloak versions must be explicit major.minor.patch releases")
    return {"keycloak": list(dict.fromkeys(versions)), "postgres": list(POSTGRES_MAJORS)}


def maven_commands(mode, matrix, keycloak_version="", postgres_major=None, extra=()):
    """Keep the local full matrix and individual CI jobs on the same test selection."""
    if mode == "quick":
        selections = [[]]
    elif mode == "security":
        selections = [["-Psbom"]]
    elif mode == "integration":
        selections = [["-Pintegration"]]
        if keycloak_version:
            selections[0].append(f"-Dkeycloak.runtime.version={keycloak_version}")
    elif mode == "postgres":
        selections = [["-Pintegration", f"-Dpostgres.image=postgres:{postgres_major}-alpine",
                       f"-Dit.test={POSTGRES_TESTS}"]]
    elif mode == "matrix":
        selections = [["-Pintegration", f"-Dkeycloak.runtime.version={version}"]
                      for version in matrix["keycloak"]]
        selections.extend(["-Pintegration", f"-Dpostgres.image=postgres:{major}-alpine",
                           f"-Dit.test={POSTGRES_TESTS}"] for major in matrix["postgres"])
    else:
        raise ValueError(f"Unknown validation mode: {mode}")
    return [["-B", "-ntp", *selection, *extra, "verify"] for selection in selections]


def run(command):
    print("+ " + subprocess.list2cmdline(command), flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", nargs="?", default="quick",
                        choices=("quick", "integration", "postgres", "matrix", "security", "matrix-json"))
    parser.add_argument("--keycloak-version", default="",
                        help="Exact runtime release; adds a candidate when running the matrix")
    parser.add_argument("--postgres-major", choices=POSTGRES_MAJORS,
                        help="Required with postgres mode")
    parser.add_argument("--maven-arg", action="append", default=[],
                        help="Extra Maven argument; repeat as --maven-arg=-Dname=value")
    args = parser.parse_args(argv)
    if args.keycloak_version and args.mode not in ("integration", "matrix", "matrix-json"):
        parser.error("--keycloak-version requires integration, matrix or matrix-json mode")
    if (args.mode == "postgres") != (args.postgres_major is not None):
        parser.error("--postgres-major is required only for postgres mode")
    try:
        matrix = compatibility_matrix(args.keycloak_version)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    if args.mode == "matrix-json":
        print(json.dumps(matrix))
        return 0
    maven = shutil.which("mvn")
    if not maven:
        parser.error("Maven 3.9+ must be on PATH; set JAVA_HOME to a JDK 21+ installation")
    try:
        run([sys.executable, "-m", "unittest", "discover", "-s", "scripts", "-p", "test_*.py"])
        for command in maven_commands(args.mode, matrix, args.keycloak_version,
                                      args.postgres_major, args.maven_arg):
            run([maven, *command])
        if args.mode == "security":
            run([sys.executable, "scripts/security-check.py"])
    except subprocess.CalledProcessError as error:
        return error.returncode
    return 0


if __name__ == "__main__":
    sys.exit(main())
