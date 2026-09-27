#!/usr/bin/env python3
"""Package reviewed build outputs and their checksums without publishing anything."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import zipfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--output", type=Path, default=Path("target/release"))
    args = parser.parse_args()
    if not re.fullmatch(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)", args.version):
        parser.error("Release version must be an explicit major.minor.patch, without SNAPSHOT")
    if not re.fullmatch(r"[0-9a-f]{40}", args.commit):
        parser.error("Commit must be a complete lowercase Git SHA")
    root = Path(__file__).resolve().parent.parent
    output = args.output.resolve()
    if output != root / "target" / "release":
        parser.error("Output must resolve to this repository's target/release directory")
    if output.exists() and any(output.iterdir()):
        parser.error("Release output must be empty; never overwrite an existing release bundle")
    artifacts = []
    for module, artifact in (("extension", "keycloak-nats-durable"),
                             ("consumer-example", "consumer-example")):
        jar = root / module / "target" / f"{artifact}-{args.version}.jar"
        with zipfile.ZipFile(jar) as archive:
            metadata = archive.read(f"META-INF/maven/io.github.gbeaule/{artifact}/pom.properties")
            properties = dict(line.split("=", 1) for line in metadata.decode().splitlines()
                              if "=" in line and not line.startswith("#"))
            if properties.get("version") != args.version:
                parser.error(f"Artifact version does not match release: {jar.name}")
        artifacts.append((jar, jar.name))
    for module in ("", "extension", "consumer-example"):
        for name in ("bom.json", "runtime-bom.json"):
            bom = root / module / "target" / name
            inventory = json.loads(bom.read_text(encoding="utf-8"))
            if inventory.get("bomFormat") != "CycloneDX" or not inventory.get("components"):
                parser.error(f"Missing or empty CycloneDX inventory: {bom}")
            if inventory.get("metadata", {}).get("component", {}).get("version") != args.version:
                parser.error(f"SBOM version does not match release: {bom}")
            artifacts.append((bom, f"{module or 'aggregate'}-{name}"))
    images = {}
    # Record configured image versions without inventing a digest for a tag.
    images["compose.yaml"] = re.findall(r"^\s+image:\s*(\S+)\s*$",
                                        (root / "compose.yaml").read_text(encoding="utf-8"), re.MULTILINE)
    if len(images["compose.yaml"]) != 2:
        parser.error("Expected PostgreSQL and NATS image references in compose.yaml")
    for path in ("deploy/Dockerfile.keycloak", "deploy/Dockerfile.consumer"):
        dockerfile = (root / path).read_text(encoding="utf-8")
        defaults = dict(re.findall(r"^ARG\s+(\w+)=(\S+)\s*$", dockerfile, re.MULTILINE))
        bases = re.findall(r"^FROM\s+(\S+)", dockerfile, re.MULTILINE)
        images[path] = sorted(set(re.sub(r"\$\{(\w+)\}",
                                        lambda match: defaults.get(match[1], match[0]), base)
                                  for base in bases))
        if not images[path] or any(not re.fullmatch(r"[\w./:-]+:[\w.-]+", image)
                                   for image in images[path]):
            parser.error(f"Expected version-tagged base image references in {path}")
    output.mkdir(parents=True, exist_ok=True)
    checksums = {}
    for source, name in artifacts:
        destination = output / name
        shutil.copyfile(source, destination)
        checksums[name] = hashlib.sha256(destination.read_bytes()).hexdigest()
    manifest = {"version": args.version, "sourceCommit": args.commit,
                "artifacts": checksums, "baseImages": images}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    checksums["manifest.json"] = hashlib.sha256((output / "manifest.json").read_bytes()).hexdigest()
    (output / "SHA256SUMS").write_text("".join(f"{digest}  {name}\n" for name, digest in
                                             sorted(checksums.items())), encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
