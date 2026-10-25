"""Privacy-first recursive scrubbing for event payloads."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from urllib.parse import unquote_plus

from closeyourit.models import CIRCULAR, FilterParameter, JsonValue

FILTERED = "[FILTERED]"

_DENYLIST = (
    "pass",
    "secret",
    "token",
    "bearer",
    "apikey",
    "authorization",
    "cookie",
    "csrf",
    "session",
    "pin",
    "credit",
    "card",
    "cvv",
    "pan",
    "iban",
    "email",
    "phone",
    "telephone",
    "mobile",
    "dob",
    "birth",
    "ssn",
    "passport",
)
_SENSITIVE_HEADERS = frozenset(
    {
        "authorization",
        "proxyauthorization",
        "cookie",
        "setcookie",
        "xapikey",
        "xauthtoken",
        "xforwardedfor",
        "xrealip",
        "trueclientip",
        "forwarded",
    }
)
_URL_KEYS = frozenset({"url", "uri", "href", "location", "referer", "referrer"})
# HTTP authentication schemes that separate the type from the value with a space. The rule is
# contractual and shared across SDKs: the value becomes `[FILTERED]`, the scheme stays visible
# when it is not already covered by a sensitive key (CYPY-15).
_AUTH_SCHEMES = "bearer|basic|digest|token|apikey"
# A sensitive key's value can start with the scheme: without consuming it here, `[^\s&#]+`
# stops at `Bearer` and leaves the credential that follows it in the message.
_ASSIGNMENT = re.compile(
    rf"([\w.\-]+)\s*[=:]\s*(?:(?:{_AUTH_SCHEMES})[ \t]+)?([^\s&#]+)",
    re.IGNORECASE,
)
# Scheme quoted without a sensitive key in front. The space is `[ \t]` and not `\s`: in a multiline
# message the scheme must not hook the first word of the next line.
_SCHEME_CREDENTIAL = re.compile(
    rf"\b({_AUTH_SCHEMES})([ \t]+)([^\s&#]+)",
    re.IGNORECASE,
)
# After a scheme everything is redacted, except what is recognizable as a sentence word: only
# lowercase letters and either short or present in this list. The rule is fail-closed because the
# shape of the value does not tell a credential from a word — `supersecretpass` and
# `authentication` are both lowercase and of similar length — and letting a secret through costs
# more than redacting one extra word. Left out are the long words that recur in error messages.
_PROSE_AFTER_SCHEME = frozenset(
    {
        "algorithm",
        "authentication",
        "authorization",
        "challenge",
        "credential",
        "credentials",
        "expected",
        "mismatch",
        "provided",
        "required",
        "supported",
        "unsupported",
    }
)
_TRAILING_PUNCTUATION = ".,;:!?)]}'\""
_MIN_OPAQUE_LENGTH = 8


def _is_prose(value: str) -> bool:
    """Recognizes the sentence word that follows the scheme, trailing punctuation excluded."""
    word = value.strip(_TRAILING_PUNCTUATION)
    if not word.isalpha() or not word.islower():
        return False
    return len(word) < _MIN_OPAQUE_LENGTH or word in _PROSE_AFTER_SCHEME


def _normalize(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).casefold())


@dataclass(frozen=True, slots=True)
class Scrubber:
    """Copy and sanitize values before they reach a queue, hook, or transport."""

    filter_parameters: tuple[FilterParameter, ...] = ()

    def is_sensitive_key(self, key: object) -> bool:
        normalized = _normalize(key)
        if any(token in normalized for token in _DENYLIST):
            return True

        return any(
            parameter.search(str(key)) is not None
            if isinstance(parameter, re.Pattern)
            else _normalize(parameter) in normalized
            for parameter in self.filter_parameters
        )

    def scrub(self, value: object) -> JsonValue:
        return self._scrub(value, seen=set())

    def scrub_message(self, message: str) -> str:
        """Redact credentials and sensitive data quoted in a free-form message.

        Two forms, both shared with the other SDKs: a pair with a sensitive key becomes
        `key=[FILTERED]` — authentication scheme included — and a scheme appearing on its own
        becomes `Bearer [FILTERED]`, so the diagnosis stays readable without the value.

        After a scheme everything that is not a known sentence word is redacted: the shape of the
        value does not tell a lowercase credential from a common word, and when in doubt it redacts.
        """
        cleaned = message.replace("\x00", "")

        def replace_assignment(match: re.Match[str]) -> str:
            key = match.group(1)
            return f"{key}={FILTERED}" if self.is_sensitive_key(key) else match.group(0)

        def replace_scheme(match: re.Match[str]) -> str:
            scheme, spacing, credential = match.groups()
            if _is_prose(credential):
                return match.group(0)
            return f"{scheme}{spacing}{FILTERED}"

        return _SCHEME_CREDENTIAL.sub(replace_scheme, _ASSIGNMENT.sub(replace_assignment, cleaned))

    def scrub_query_string(self, query: str) -> str:
        pairs: list[str] = []
        for pair in query.replace("\x00", "").split("&"):
            key, separator, _value = pair.partition("=")
            if self.is_sensitive_key(unquote_plus(key)):
                pairs.append(f"{key}={FILTERED}")
            else:
                pairs.append(pair if separator else key)
        return "&".join(pairs)

    def strip_url_query(self, url: str) -> str:
        return url.replace("\x00", "").split("?", 1)[0].split("#", 1)[0]

    def scrub_headers(self, headers: Mapping[object, object]) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {}
        for name, value in headers.items():
            key = str(name).replace("\x00", "")
            result[key] = (
                FILTERED
                if _normalize(key) in _SENSITIVE_HEADERS or self.is_sensitive_key(key)
                else self._scrub(value, seen=set())
            )
        return result

    def _scrub(self, value: object, *, seen: set[int]) -> JsonValue:
        if value is None or isinstance(value, bool | int | float):
            return value
        if isinstance(value, str):
            return value.replace("\x00", "")
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace").replace("\x00", "")

        if isinstance(value, Mapping):
            identity = id(value)
            if identity in seen:
                return CIRCULAR
            seen.add(identity)
            try:
                result: dict[str, JsonValue] = {}
                for raw_key, item in value.items():
                    key = str(raw_key).replace("\x00", "")
                    normalized = _normalize(key)
                    if self.is_sensitive_key(key):
                        result[key] = FILTERED
                    elif normalized in _URL_KEYS and isinstance(item, str):
                        result[key] = self.strip_url_query(item)
                    elif normalized == "querystring" and isinstance(item, str):
                        result[key] = self.scrub_query_string(item)
                    elif normalized == "headers" and isinstance(item, Mapping):
                        result[key] = self.scrub_headers(item)
                    else:
                        result[key] = self._scrub(item, seen=seen)
                return result
            finally:
                seen.remove(identity)

        if isinstance(value, Sequence):
            identity = id(value)
            if identity in seen:
                return CIRCULAR
            seen.add(identity)
            try:
                return [self._scrub(item, seen=seen) for item in value]
            finally:
                seen.remove(identity)

        return f"<{type(value).__name__}>"
