"""Validate release metadata before building publishable distributions."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from closeyourit import __version__

_SEMVER_COMPONENT = r"(?:0|[1-9]\d*)"
_STABLE_TAG = re.compile(
    rf"v(?P<version>{_SEMVER_COMPONENT}\.{_SEMVER_COMPONENT}\.{_SEMVER_COMPONENT})\Z"
)


def validate_release(*, tag: str, package_version: str, changelog: str) -> list[str]:
    """Return every release metadata error in deterministic order."""
    match = _STABLE_TAG.fullmatch(tag)
    if match is None:
        # CYPY-12 — the received tag goes INSIDE the message, on the same line as the reason.
        # Whoever reads a run's log — a person or the machine checking its outcome — must
        # understand from that single line which tag arrived: a tag taken from the command
        # header or from the checkout would describe a different step.
        return [
            f"tag {tag} is not stable: "
            "this repository only publishes stable vMAJOR.MINOR.PATCH tags"
        ]

    tag_version = match.group("version")
    errors: list[str] = []

    if package_version != tag_version:
        errors.append(f"package version {package_version} does not match tag {tag}")

    changelog_heading = re.compile(rf"^## \[{re.escape(tag_version)}\](?:\s|$)", re.MULTILINE)
    if changelog_heading.search(changelog) is None:
        errors.append(f"CHANGELOG.md does not contain the section ## [{tag_version}]")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True, help="Expected Git tag, for example v0.1.0")
    parser.add_argument("--changelog", type=Path, default=Path("CHANGELOG.md"))
    arguments = parser.parse_args()

    errors = validate_release(
        tag=arguments.tag,
        package_version=__version__,
        changelog=arguments.changelog.read_text(encoding="utf-8"),
    )
    for error in errors:
        print(f"invalid release: {error}")

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
