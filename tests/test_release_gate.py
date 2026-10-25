from scripts.check_release import validate_release


def test_release_gate_accepts_matching_stable_release() -> None:
    errors = validate_release(
        tag="v0.1.0",
        package_version="0.1.0",
        changelog="# Changelog\n\n## [0.1.0] - 2026-07-13\n",
    )

    assert errors == []


def test_release_gate_rejects_prerelease_tag() -> None:
    errors = validate_release(
        tag="v0.1.0-rc1",
        package_version="0.1.0",
        changelog="## [0.1.0]\n",
    )

    # CYPY-12 — the message must name the received tag: whoever reads the run's log must
    # understand in ONE line which tag arrived and why it was rejected, without having to look for
    # the tag in the command header.
    assert errors == [
        "tag v0.1.0-rc1 is not stable: "
        "this repository only publishes stable vMAJOR.MINOR.PATCH tags"
    ]


def test_release_gate_reports_version_mismatch_and_missing_changelog_entry() -> None:
    errors = validate_release(
        tag="v0.2.0",
        package_version="0.1.0",
        changelog="## [Unreleased]\n",
    )

    assert errors == [
        "package version 0.1.0 does not match tag v0.2.0",
        "CHANGELOG.md does not contain the section ## [0.2.0]",
    ]


def test_release_gate_rejects_semver_components_with_leading_zeroes() -> None:
    errors = validate_release(
        tag="v01.2.3",
        package_version="01.2.3",
        changelog="## [01.2.3]\n",
    )

    assert errors == [
        "tag v01.2.3 is not stable: this repository only publishes stable vMAJOR.MINOR.PATCH tags"
    ]


def test_release_gate_rejects_a_tag_with_four_components() -> None:
    # CYPY-12 — a malformed tag must not vanish into an empty run: it is rejected and the line
    # says which one it is.
    errors = validate_release(
        tag="v1.2.3.4",
        package_version="1.2.3",
        changelog="## [1.2.3]\n",
    )

    assert errors == [
        "tag v1.2.3.4 is not stable: this repository only publishes stable vMAJOR.MINOR.PATCH tags"
    ]
