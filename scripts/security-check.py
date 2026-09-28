#!/usr/bin/env python3
"""Gate shipped dependencies; report owner-accepted upstream image findings without blocking."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess

SCANNER = "aquasec/trivy:0.74.0"
BLOCKING_REPORT = "runtime-dependencies.json"


def evaluate(outputs):
    """Retain findings and their scanner severity; acceptance changes the gate, not the report."""
    if not any(output.name == BLOCKING_REPORT for output in outputs):
        raise ValueError("Missing shipped runtime dependency scan")
    blocking, advisory = [], []
    for output in outputs:
        report = json.loads(output.read_text(encoding="utf-8"))
        if (not isinstance(report, dict) or report.get("SchemaVersion") != 2
                or not isinstance(report.get("Results"), list)
                or any(not isinstance(result, dict) for result in report["Results"])):
            raise ValueError(f"Incomplete scanner report: {output.name}")
        if output.name == BLOCKING_REPORT and not any(
                result.get("Class") == "lang-pkgs" and result.get("Type") == "jar"
                and isinstance(result.get("Packages"), list) and result["Packages"]
                for result in report["Results"]):
            raise ValueError("Shipped runtime scan contains no Java package inventory")
        for result in report["Results"]:
            for finding in result.get("Vulnerabilities", []) or []:
                if finding.get("Severity") in ("HIGH", "CRITICAL") and finding.get("FixedVersion"):
                    entry = {"report": output.name, "id": finding["VulnerabilityID"],
                             "package": finding["PkgName"], "installed": finding["InstalledVersion"],
                             "fixed": finding["FixedVersion"], "severity": finding["Severity"]}
                    (blocking if output.name == BLOCKING_REPORT else advisory).append(entry)
    return {"scanner": SCANNER, "completedAtUtc": datetime.now(timezone.utc).isoformat(),
            "blockingFindings": blocking, "advisoryFindings": advisory,
            "policy": "Block fixed HIGH/CRITICAL shipped dependencies; upstream images and provided "
                      "Keycloak dependencies are owner-accepted advisory findings; retain all severities"}


def run(*command):
    subprocess.run(command, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", default="1.0.0-SNAPSHOT")
    args = parser.parse_args()
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:-SNAPSHOT)?", args.version):
        parser.error("Expected a numeric Maven version, optionally ending in -SNAPSHOT")
    root = Path(__file__).resolve().parent.parent
    reports = root / "target" / "security"
    cache = root / ".work" / "trivy-cache"
    reports.mkdir(parents=True, exist_ok=True)
    cache.mkdir(parents=True, exist_ok=True)
    # A failed new scan must not leave an old gate verdict looking current.
    (reports / "gate.json").unlink(missing_ok=True)
    for name in ("bom.json", "runtime-bom.json"):
        bom = root / "target" / name
        if not bom.is_file():
            parser.error("Run mvn -Psbom verify first; both complete and runtime inventories are required")
        inventory = json.loads(bom.read_text(encoding="utf-8"))
        if not inventory.get("components") or inventory.get("metadata", {}).get("component", {}).get("version") != args.version:
            parser.error(f"Empty or mismatched inventory: {name}")
    base = ["docker", "run", "--rm", "--mount", f"type=bind,src={root},dst=/workspace,readonly",
            "--mount", f"type=bind,src={reports},dst=/reports",
            "--mount", f"type=bind,src={cache},dst=/root/.cache/trivy", SCANNER]
    shared = ["--format", "json", "--timeout", "10m", "--exit-code", "0"]
    run(*base, "sbom", *shared, "--output", "/reports/dependencies.json", "/workspace/target/bom.json")
    run(*base, "sbom", *shared, "--output", f"/reports/{BLOCKING_REPORT}",
        "/workspace/target/runtime-bom.json")
    outputs = [reports / "dependencies.json", reports / BLOCKING_REPORT]
    for name in ("consumer", "keycloak"):
        image = f"knd-security-{name}:local"
        run("docker", "build", "--build-arg", f"ARTIFACT_VERSION={args.version}",
            "--file", str(root / "deploy" / f"Dockerfile.{name}"), "--tag", image, str(root))
        archive = reports / f"{name}.tar"
        run("docker", "image", "save", "--output", str(archive), image)
        try:
            run(*base, "image", *shared, "--scanners", "vuln", "--output", f"/reports/{name}.json",
                "--input", f"/reports/{name}.tar")
        finally:
            archive.unlink(missing_ok=True)
        outputs.append(reports / f"{name}.json")
    images = re.findall(r"^\s+image:\s*((?:postgres|nats):[\w.-]+(?:@sha256:[0-9a-f]{64})?)\s*$",
                        (root / "compose.yaml").read_text(encoding="utf-8"), re.MULTILINE)
    if len(images) != 2:
        raise SystemExit("Expected versioned PostgreSQL and NATS images in compose.yaml")
    provisioner = re.findall(r"^\s+image:\s*(natsio/nats-box:[\w.-]+(?:@sha256:[0-9a-f]{64})?)\s*$",
                            (root / "compose.producer.yaml").read_text(encoding="utf-8"), re.MULTILINE)
    if len(provisioner) != 1:
        raise SystemExit("Expected a versioned NATS provisioner image in compose.producer.yaml")
    images.extend(provisioner)
    for index, image in enumerate(images):
        name = f"infrastructure-{index}"
        run(*base, "image", *shared, "--scanners", "vuln", "--output", f"/reports/{name}.json", image)
        outputs.append(reports / f"{name}.json")
    gate = evaluate(outputs)
    (reports / "gate.json").write_text(json.dumps(gate, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(gate, indent=2))
    raise SystemExit(1 if gate["blockingFindings"] else 0)


if __name__ == "__main__":
    main()
