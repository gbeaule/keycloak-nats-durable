#!/usr/bin/env python3
"""Check license and notice content in the actual shaded artifacts."""

import argparse
from pathlib import Path
import sys
import zipfile

MODULES = (("extension", "keycloak-nats-durable"), ("consumer-example", "consumer-example"))
LICENSES = ("Apache-2.0.txt", "Checker-qual-MIT.txt", "THIRD-PARTY-NOTICES.txt")


def license_assets(root):
    """Use the same reviewed files for JAR resources and the standalone bundle."""
    return [(root / "LICENSE", "LICENSE", "META-INF/licenses/project/LICENSE"),
            *((root / "licenses" / name, "licenses/" + name, "META-INF/licenses/" + name)
              for name in LICENSES)]


def check_jar(root, jar, module):
    with zipfile.ZipFile(jar) as archive:
        for source, _, resource in license_assets(root):
            content = source.read_bytes()
            if not content.strip() or archive.read(resource) != content:
                raise ValueError(f"Missing or changed license content: {jar.name}:{resource}")
        apache = archive.read("META-INF/licenses/Apache-2.0.txt")
        if any(fragment not in apache for fragment in
               (b"Grant of Copyright License", b"Grant of Patent License", b"END OF TERMS AND CONDITIONS")):
            raise ValueError(f"Incomplete Apache-2.0 license: {jar.name}")
        bc = ("io/github/gbeaule/keycloaknats/internal/bouncycastle/" if module == "extension"
              else "org/bouncycastle/") + "LICENSE.class"
        checks = {bc: (b"Copyright (c)", b"Permission is hereby granted", b"SOFTWARE IS PROVIDED")}
        if module == "consumer-example":
            checks.update({
                "META-INF/LICENSE": (b"PostgreSQL Global Development Group",
                                     b"Grant of Patent License", b"END OF TERMS AND CONDITIONS"),
                "META-INF/LICENSE.txt": (b"QOS.ch", b"Permission is hereby granted"),
                "META-INF/NOTICE": (b"Jackson JSON processor", b"FastDoubleParser", b"Schubfach"),
                "META-INF/FastDoubleParser-LICENSE": (b"MIT License", b"Werner Randelshofer"),
                "META-INF/FastDoubleParser-ThirdParty-LICENSE": (b"FastDoubleParser",),
                "META-INF/Schubfach-LICENSE": (b"Raffaello Giulietti", b"Permission is hereby granted"),
            })
            for component in ("scram-client", "scram-common", "saslprep", "stringprep"):
                if not any(name.startswith("META-INF/licenses/com.ongres.")
                           and f"/{component}-" in name and name.endswith("/META-INF/LICENSE")
                           for name in archive.namelist()):
                    raise ValueError(f"Missing PostgreSQL dependency license: {jar.name}:{component}")
        for resource, fragments in checks.items():
            content = archive.read(resource)
            if any(fragment not in content for fragment in fragments):
                raise ValueError(f"Incomplete dependency notice: {jar.name}:{resource}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", default="")
    parser.add_argument("--built-after", type=int, default=0,
                        help="Reject artifacts not rebuilt since this Unix timestamp in nanoseconds")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parent.parent
    try:
        for module, artifact in MODULES:
            jars = [jar for jar in (root / module / "target").glob(artifact + "-*.jar")
                    if not jar.name.endswith("-shaded.jar")
                    and (not args.version or jar.name == f"{artifact}-{args.version}.jar")
                    and jar.stat().st_mtime_ns >= args.built_after]
            if len(jars) != 1:
                raise ValueError(f"Expected one freshly packaged {artifact} JAR; found {len(jars)}. "
                                 "Validation requires packaging both shaded artifacts.")
            check_jar(root, jars[0], module)
    except (OSError, ValueError, KeyError, zipfile.BadZipFile) as error:
        parser.exit(1, f"Packaged license check failed: {error}\n")
    print("Both shaded artifacts retain complete licenses and dependency notices.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
