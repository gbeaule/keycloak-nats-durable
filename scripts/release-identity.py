#!/usr/bin/env python3
"""Validate a candidate version and export its reproducible build identity for GitHub Actions."""

import os
from pathlib import Path
import re
import subprocess


def main():
    version = os.environ.get("REQUESTED_VERSION", "")
    if os.environ.get("RELEASE_REF_TYPE") == "tag":
        version = os.environ.get("RELEASE_REF", "").removeprefix("v")
    if not re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", version):
        raise SystemExit("Release version must be major.minor.patch")
    epoch = subprocess.check_output(
        ["git", "show", "-s", "--format=%ct", "HEAD"], text=True
    ).strip()
    if not re.fullmatch(r"[0-9]+", epoch):
        raise SystemExit("Source commit timestamp must be an integer")
    with Path(os.environ["GITHUB_ENV"]).open("a", encoding="utf-8") as target:
        target.write(f"RELEASE_VERSION={version}\nSOURCE_DATE_EPOCH={epoch}\n")


if __name__ == "__main__":
    main()
