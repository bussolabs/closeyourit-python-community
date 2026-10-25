from __future__ import annotations

import re

import pytest

from closeyourit.scrubber import FILTERED, Scrubber


@pytest.mark.parametrize(
    "key",
    [
        "password",
        "client_secret",
        "access_token",
        "bearer_token",
        "api_key",
        "authorization",
        "set-cookie",
        "session_id",
        "pin_code",
        "credit_card",
        "cvv",
        "card_pan",
        "iban",
        "user_email",
        "phone_number",
        "telephone",
        "mobile_number",
        "date_of_birth",
        "ssn",
        "passport_number",
    ],
)
def test_scrubber_redacts_every_canonical_sensitive_key(key: str) -> None:
    assert Scrubber().scrub({key: "private"}) == {key: FILTERED}


def test_scrubber_handles_zero_one_and_many_nested_items_without_mutating_input() -> None:
    original = {
        "empty": [],
        "single": [{"token": "one"}],
        "many": [{"name": "safe"}, {"card_number": "4242"}],
    }

    result = Scrubber().scrub(original)

    assert result == {
        "empty": [],
        "single": [{"token": FILTERED}],
        "many": [{"name": "safe"}, {"card_number": FILTERED}],
    }
    assert original["single"] == [{"token": "one"}]


def test_scrubber_applies_custom_string_and_regex_filters() -> None:
    scrubber = Scrubber(filter_parameters=("customer_reference", re.compile(r"^private_")))

    assert scrubber.scrub(
        {"customer-reference": "one", "private_note": "two", "public_note": "three"}
    ) == {
        "customer-reference": FILTERED,
        "private_note": FILTERED,
        "public_note": "three",
    }


def test_scrubber_sanitizes_complete_request_context() -> None:
    request = {
        "url": "https://example.com/orders?token=abc&view=full",
        "query_string": "token=abc&view=full&email=a%40b.com",
        "headers": {
            "accept": "application/json",
            "authorization": "Bearer abc",
            "X-Forwarded-For": "203.0.113.8",
        },
        "data": {"items": [{"card_number": "4242", "quantity": 1}]},
    }

    result = Scrubber().scrub(request)

    assert result == {
        "url": "https://example.com/orders",
        "query_string": f"token={FILTERED}&view=full&email={FILTERED}",
        "headers": {
            "accept": "application/json",
            "authorization": FILTERED,
            "X-Forwarded-For": FILTERED,
        },
        "data": {"items": [{"card_number": FILTERED, "quantity": 1}]},
    }


def test_scrubber_strips_queries_from_nested_url_keys() -> None:
    assert Scrubber().scrub(
        {"data": {"href": "https://example.com/path?email=a@b.com", "label": "safe"}}
    ) == {"data": {"href": "https://example.com/path", "label": "safe"}}


def test_scrubber_redacts_sensitive_assignments_in_messages() -> None:
    scrubber = Scrubber()

    assert scrubber.scrub_message("login password=hunter2 token: abc") == (
        f"login password={FILTERED} token={FILTERED}"
    )
    assert scrubber.scrub_message("password reset requested") == "password reset requested"


def test_scrubber_removes_null_bytes_recursively() -> None:
    assert Scrubber().scrub({"message": "bad\x00text", "items": ["a\x00b"]}) == {
        "message": "badtext",
        "items": ["ab"],
    }


def test_scrubber_handles_circular_structures_without_recursing_forever() -> None:
    value: dict[str, object] = {"id": 1}
    value["self"] = value

    assert Scrubber().scrub(value) == {"id": 1, "self": "[Circular]"}


def test_scrubber_normalizes_bytes_and_unknown_values() -> None:
    scrubber = Scrubber()

    assert scrubber.scrub(b"before\x00after") == "beforeafter"
    assert scrubber.scrub(object()) == "<object>"


def test_scrubber_handles_circular_sequences_without_recursing_forever() -> None:
    circular: list[object] = []
    circular.append(circular)

    assert Scrubber().scrub(circular) == ["[Circular]"]


# CYPY-15 — an access header quoted inside a free-form message. The assignment filter
# stopped at the first space: it hid the scheme (`Bearer`) and let the credential that
# follows it through. The cases below use placeholders, never real credentials.
EXAMPLE_CREDENTIAL = "EXAMPLE_NOT_A_CREDENTIAL"


def test_scrubber_redacts_the_credential_that_follows_an_auth_scheme() -> None:
    """An access header quoted in an error does not transmit the value after Bearer."""
    scrubber = Scrubber()

    assert scrubber.scrub_message(f"Authorization: Bearer {EXAMPLE_CREDENTIAL}") == (
        f"Authorization={FILTERED}"
    )


def test_scrubber_keeps_the_diagnostic_text_around_a_redacted_header() -> None:
    """The non-sensitive surroundings stay readable: the credential is lost, not the diagnosis."""
    result = Scrubber().scrub_message(
        f"401 while calling /orders with Authorization: Bearer {EXAMPLE_CREDENTIAL}"
    )

    assert EXAMPLE_CREDENTIAL not in result
    assert result.startswith("401 while calling /orders with")


@pytest.mark.parametrize(
    "message",
    [
        "Authorization: Bearer {credential}",
        "authorization:bearer {credential}",
        "AUTHORIZATION:   BEARER   {credential}",
        "Authorization: Basic {credential}",
        "Proxy-Authorization: Digest {credential}",
        "X-Api-Key: Token {credential}",
    ],
)
def test_scrubber_redacts_auth_schemes_regardless_of_case_and_spacing(message: str) -> None:
    """Uppercase, multiple spaces, missing space and an alternative scheme open no loophole."""
    result = Scrubber().scrub_message(message.format(credential=EXAMPLE_CREDENTIAL))

    assert EXAMPLE_CREDENTIAL not in result
    assert result.endswith(FILTERED)


def test_scrubber_redacts_an_auth_scheme_carried_by_a_non_sensitive_key() -> None:
    """The scheme stays readable for diagnosis, the credential does not."""
    assert Scrubber().scrub_message(f"retry with Bearer {EXAMPLE_CREDENTIAL} then stop") == (
        f"retry with Bearer {FILTERED} then stop"
    )


@pytest.mark.parametrize(
    "credential",
    ["dXNlcjpwYXNz", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln", "abcdefghijklmnopqrstuvwx"],
)
def test_scrubber_recognizes_opaque_credential_shapes(credential: str) -> None:
    """Base64, JWT and long opaque tokens count as credentials even without a key."""
    assert Scrubber().scrub_message(f"header was Bearer {credential}") == (
        f"header was Bearer {FILTERED}"
    )


@pytest.mark.parametrize(
    "message",
    [
        "the bearer of bad news arrived",
        "Basic authentication failed",
        "Token expected but missing",
        "Digest algorithm unsupported",
    ],
)
def test_scrubber_leaves_ordinary_prose_untouched(message: str) -> None:
    """A common word after the scheme is prose, not a credential: it must not be redacted."""
    assert Scrubber().scrub_message(message) == message


def test_scrubber_redacts_auth_schemes_line_by_line_in_multiline_messages() -> None:
    """Across lines the redaction does not cross the line nor erase the diagnostic text."""
    message = (
        f"Authorization: Bearer {EXAMPLE_CREDENTIAL}\n"
        "GET /orders returned 401\n"
        "missing Bearer\ntoken in the retry"
    )

    assert Scrubber().scrub_message(message) == (
        f"Authorization={FILTERED}\nGET /orders returned 401\nmissing Bearer\ntoken in the retry"
    )


def test_scrubber_does_not_amplify_a_redacted_message() -> None:
    """The redacted text does not inflate the diagnostic payload compared to the original."""
    message = f"Authorization: Bearer {'a1' * 64}"

    assert len(Scrubber().scrub_message(message)) < len(message)


def test_scrubber_keeps_structured_headers_redacted_by_key() -> None:
    """The structured header stays protected by key: the new rule does not weaken it."""
    assert Scrubber().scrub({"headers": {"Authorization": f"Bearer {EXAMPLE_CREDENTIAL}"}}) == {
        "headers": {"Authorization": FILTERED}
    }


@pytest.mark.parametrize("credential", ["supersecretpass", "mysecretvalue", "chiavesegretissima"])
def test_scrubber_redacts_lowercase_credentials_after_a_scheme(credential: str) -> None:
    """An all-lowercase credential looks no different from a word: it is redacted anyway."""
    assert Scrubber().scrub_message(f"header was Bearer {credential}") == (
        f"header was Bearer {FILTERED}"
    )
    assert Scrubber().scrub_message(f"Token {credential} rejected") == (
        f"Token {FILTERED} rejected"
    )


def test_scrubber_leaves_prose_untouched_when_it_carries_final_punctuation() -> None:
    """Trailing punctuation must not turn a sentence word into a credential."""
    assert Scrubber().scrub_message("Basic authentication.") == "Basic authentication."
