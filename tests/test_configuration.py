from __future__ import annotations

import logging
from uuid import uuid4

import pytest

from closeyourit.configuration import Configuration


def valid_configuration(**overrides: object) -> Configuration:
    values: dict[str, object] = {
        "endpoint_url": "https://ingest.example.com",
        "token": "cyi_test_token",
        "project_id": str(uuid4()),
        "environment": "production",
    }
    values.update(overrides)
    return Configuration(**values)  # type: ignore[arg-type]


def test_configuration_reads_consumer_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    project_id = str(uuid4())
    monkeypatch.setenv("CLOSEYOURIT_ENDPOINT_URL", "https://ingest.example.com/")
    monkeypatch.setenv("CLOSEYOURIT_TOKEN", "cyi_from_environment")
    monkeypatch.setenv("CLOSEYOURIT_PROJECT_ID", project_id)
    monkeypatch.setenv("CLOSEYOURIT_ENVIRONMENT", "Staging")
    monkeypatch.setenv("CLOSEYOURIT_RELEASE", "v2.3.4")

    configuration = Configuration()

    assert configuration.endpoint_url == "https://ingest.example.com"
    assert configuration.token == "cyi_from_environment"
    assert configuration.project_id == project_id
    assert configuration.environment == "staging"
    assert configuration.release == "v2.3.4"
    assert configuration.enabled is True
    assert configuration.disabled_reason is None


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("endpoint_url", None, "missing_endpoint_url"),
        ("endpoint_url", "   ", "missing_endpoint_url"),
        ("token", None, "missing_token"),
        ("token", "", "missing_token"),
        ("project_id", None, "missing_project_id"),
        ("project_id", "  ", "missing_project_id"),
    ],
)
def test_configuration_is_noop_when_required_values_are_blank(
    field: str, value: object, reason: str
) -> None:
    configuration = valid_configuration(**{field: value})

    assert configuration.enabled is False
    assert configuration.disabled_reason == reason


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"endpoint_url": "not a url"}, "invalid_endpoint_url"),
        ({"endpoint_url": "ftp://ingest.example.com"}, "invalid_endpoint_url"),
        ({"token": "public_or_user_token"}, "invalid_token"),
        ({"project_id": "project-slug"}, "invalid_project_id"),
    ],
)
def test_configuration_is_noop_for_malformed_credentials(
    overrides: dict[str, object], reason: str
) -> None:
    configuration = valid_configuration(**overrides)

    assert configuration.enabled is False
    assert configuration.disabled_reason == reason


def test_configuration_rejects_plain_http_in_production() -> None:
    configuration = valid_configuration(endpoint_url="http://ingest.example.com")

    assert configuration.enabled is False
    assert configuration.disabled_reason == "insecure_endpoint_in_production"


def test_configuration_allows_plain_http_outside_production() -> None:
    configuration = valid_configuration(
        endpoint_url="http://localhost:3011", environment="development"
    )

    assert configuration.enabled is True


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "[::1]"])
def test_configuration_accepts_plain_http_on_loopback_without_a_warning(
    host: str, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        configuration = valid_configuration(
            endpoint_url=f"http://{host}:3011", environment="development"
        )

    assert configuration.enabled is True
    assert [record for record in caplog.records if record.name == "closeyourit"] == []


def test_configuration_warns_about_plain_http_on_a_remote_host_outside_production(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        configuration = valid_configuration(
            endpoint_url="http://ingest.example.com", environment="staging"
        )

    assert configuration.enabled is True
    warnings = [record for record in caplog.records if record.name == "closeyourit"]
    assert len(warnings) == 1
    assert warnings[0].levelno == logging.WARNING
    assert "ingest.example.com" in warnings[0].getMessage()
    assert "https" in warnings[0].getMessage()


def test_before_send_receives_a_copy_and_can_transform_an_event() -> None:
    event: dict[str, object] = {"message": "before", "extra": {"count": 1}}

    def transform(payload: dict[str, object]) -> dict[str, object]:
        payload["message"] = "after"
        return payload

    configuration = valid_configuration(before_send=transform)

    assert configuration.apply_before_send(event) == {
        "message": "after",
        "extra": {"count": 1},
    }
    assert event == {"message": "before", "extra": {"count": 1}}


def test_before_send_returns_a_detached_copy_when_no_hook_is_configured() -> None:
    source: dict[str, object] = {"context": {"attempt": 1}}

    result = valid_configuration().apply_before_send(source)

    assert result == source
    assert result is not source
    assert result is not None
    assert result["context"] is not source["context"]


def test_before_send_failure_drops_the_event_without_raising() -> None:
    def fail(_payload: dict[str, object]) -> dict[str, object]:
        raise RuntimeError("consumer callback failed")

    configuration = valid_configuration(before_send=fail)

    assert configuration.apply_before_send({"message": "safe"}) is None


def test_before_send_can_deliberately_drop_the_event() -> None:
    configuration = valid_configuration(before_send=lambda _payload: None)

    assert configuration.apply_before_send({"message": "safe"}) is None


def test_project_id_must_use_canonical_uuid_format() -> None:
    configuration = valid_configuration(project_id="{12345678-1234-1234-1234-123456789abc}")

    assert configuration.disabled_reason == "invalid_project_id"


def test_capture_limits_are_normalized_to_safe_boundaries() -> None:
    configuration = valid_configuration(
        sample_rate=-1,
        logs_sample_rate=2,
        max_breadcrumbs=-1,
        slow_method_threshold_ms=-1,
        slow_query_threshold_ms=-1,
        slow_external_threshold_ms=-1,
        repeated_http_threshold=0,
        repeated_http_window_ms=-1,
        n_plus_one_threshold=-1,
        query_count_threshold=-1,
        http_capture_hosts=(" API.EXAMPLE.TEST ", ""),
        trace_propagation_hosts=(" TRACE.EXAMPLE.TEST ", ""),
    )

    assert configuration.sample_rate == 0
    assert configuration.logs_sample_rate == 1
    assert configuration.max_breadcrumbs == 0
    assert configuration.slow_method_threshold_ms == 0
    assert configuration.slow_query_threshold_ms == 0
    assert configuration.slow_external_threshold_ms == 0
    assert configuration.repeated_http_threshold == 1
    assert configuration.repeated_http_window_ms == 0
    assert configuration.n_plus_one_threshold == 0
    assert configuration.query_count_threshold == 0
    assert configuration.http_capture_hosts == ("api.example.test",)
    assert configuration.trace_propagation_hosts == ("trace.example.test",)


_RELEASE_ENV_VARS = (
    "CLOSEYOURIT_RELEASE",
    "APP_GIT_TAG",
    "GIT_TAG",
    "KAMAL_VERSION",
    "GIT_SHA",
    "GIT_REVISION",
    "SOURCE_VERSION",
    "HEROKU_SLUG_COMMIT",
)


@pytest.fixture
def clean_release_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Deterministic environment: removes every release detection source before the test."""
    for name in _RELEASE_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_explicit_release_via_env_takes_precedence_over_tag_and_sha(
    clean_release_env: pytest.MonkeyPatch,
) -> None:
    clean_release_env.setenv("CLOSEYOURIT_RELEASE", "custom-release")
    clean_release_env.setenv("APP_GIT_TAG", "v9.9.9")
    clean_release_env.setenv("KAMAL_VERSION", "d3ce2af")

    assert Configuration().release == "custom-release"


def test_explicit_release_kwarg_takes_precedence_over_autodetection(
    clean_release_env: pytest.MonkeyPatch,
) -> None:
    clean_release_env.setenv("APP_GIT_TAG", "v9.9.9")

    assert valid_configuration(release="manuale").release == "manuale"


def test_release_prefers_the_semver_tag_over_the_short_sha(
    clean_release_env: pytest.MonkeyPatch,
) -> None:
    clean_release_env.setenv("APP_GIT_TAG", "v0.0.43")
    clean_release_env.setenv("KAMAL_VERSION", "d3ce2af")

    assert Configuration().release == "v0.0.43"


def test_release_uses_git_tag_when_app_git_tag_is_missing(
    clean_release_env: pytest.MonkeyPatch,
) -> None:
    clean_release_env.setenv("GIT_TAG", "v2.5.0")
    clean_release_env.setenv("KAMAL_VERSION", "d3ce2af")

    assert Configuration().release == "v2.5.0"


def test_release_accepts_a_valid_prerelease_tag(
    clean_release_env: pytest.MonkeyPatch,
) -> None:
    clean_release_env.setenv("APP_GIT_TAG", "v1.2.3-rc1")
    clean_release_env.setenv("KAMAL_VERSION", "d3ce2af")

    assert Configuration().release == "v1.2.3-rc1"


@pytest.mark.parametrize("bad", ["unknown", "", "   ", "main"])
def test_release_ignores_a_non_semver_tag_and_falls_back_to_the_short_sha(
    clean_release_env: pytest.MonkeyPatch, bad: str
) -> None:
    clean_release_env.setenv("APP_GIT_TAG", bad)
    clean_release_env.setenv("KAMAL_VERSION", "d3ce2af")

    assert Configuration().release == "d3ce2af"


def test_release_non_semver_git_tag_falls_back_to_kamal_version(
    clean_release_env: pytest.MonkeyPatch,
) -> None:
    clean_release_env.setenv("GIT_TAG", "main")
    clean_release_env.setenv("KAMAL_VERSION", "d3ce2af")

    assert Configuration().release == "d3ce2af"


def test_release_uses_kamal_version_when_there_is_no_tag(
    clean_release_env: pytest.MonkeyPatch,
) -> None:
    clean_release_env.setenv("KAMAL_VERSION", "v1.2.3")

    assert Configuration().release == "v1.2.3"


@pytest.mark.parametrize(
    "source", ["KAMAL_VERSION", "GIT_SHA", "GIT_REVISION", "SOURCE_VERSION", "HEROKU_SLUG_COMMIT"]
)
def test_release_uses_every_sha_source_of_the_chain(
    clean_release_env: pytest.MonkeyPatch, source: str
) -> None:
    clean_release_env.setenv(source, "cafe123")

    assert Configuration().release == "cafe123"


def test_release_respects_the_sha_chain_order(
    clean_release_env: pytest.MonkeyPatch,
) -> None:
    clean_release_env.setenv("KAMAL_VERSION", "sha_kamal")
    clean_release_env.setenv("GIT_SHA", "sha_git")
    clean_release_env.setenv("GIT_REVISION", "sha_revision")
    clean_release_env.setenv("SOURCE_VERSION", "sha_source")
    clean_release_env.setenv("HEROKU_SLUG_COMMIT", "sha_heroku")

    assert Configuration().release == "sha_kamal"


def test_release_is_none_without_any_source(
    clean_release_env: pytest.MonkeyPatch,
) -> None:
    assert Configuration().release is None


@pytest.mark.parametrize(
    ("value", "enabled"),
    [("1", True), ("true", True), ("YES", True), (" On ", True), ("0", False), ("", False)],
)
def test_usage_telemetry_reads_its_opt_in_flag_from_the_environment(
    monkeypatch: pytest.MonkeyPatch, value: str, enabled: bool
) -> None:
    monkeypatch.setenv("CLOSEYOURIT_USAGE_ENABLED", value)

    assert Configuration().usage_enabled is enabled


def test_usage_telemetry_stays_off_without_an_explicit_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CLOSEYOURIT_USAGE_ENABLED", raising=False)

    configuration = Configuration()

    assert configuration.usage_enabled is False
    assert configuration.usage_flush_interval == 300.0
    assert configuration.usage_max_symbols == 2000


@pytest.mark.parametrize(
    ("interval", "expected"), [(0.0, 300.0), (-5.0, 300.0), (0.5, 0.5), (60.0, 60.0)]
)
def test_a_non_positive_usage_interval_falls_back_to_the_shared_default(
    interval: float, expected: float
) -> None:
    assert Configuration(usage_flush_interval=interval).usage_flush_interval == expected


def test_a_negative_usage_cap_becomes_zero() -> None:
    assert Configuration(usage_max_symbols=-10).usage_max_symbols == 0
