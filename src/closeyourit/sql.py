"""Dependency-free SQL normalization shared by builders and integrations."""

from __future__ import annotations

import re

_TAGGED_DOLLAR_STRING = re.compile(r"\$(?P<tag>[A-Za-z_][A-Za-z0-9_]*)\$.*?\$(?P=tag)\$", re.DOTALL)
_UNTAGGED_DOLLAR_STRING = re.compile(r"\$\$.*?\$\$", re.DOTALL)
_HEX_LITERAL = re.compile(r"\b[xX]'(?:[0-9a-fA-F]{2})*'")
_STRING_LITERAL = re.compile(r"'(?:''|\\.|[^'\\])*'", re.DOTALL)
_UUID_LITERAL = re.compile(
    r"(?<![0-9a-f])[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?![0-9a-f])",
    re.IGNORECASE,
)
_HEX_NUMBER = re.compile(r"\b0x[0-9a-f]+\b", re.IGNORECASE)
_NUMBER_LITERAL = re.compile(r"(?<![\w$])[-+]?\d+(?:\.\d+)?(?:e[-+]?\d+)?\b", re.IGNORECASE)
_NAMED_BIND = re.compile(r"(?<!:):[A-Za-z_][A-Za-z0-9_]*|%\([^)]+\)s")
_WHITESPACE = re.compile(r"\s+")


def _without_comments(statement: str) -> str:
    """Scan nested comments without interpreting delimiters inside quoted values."""
    parts: list[str] = []
    index = 0
    while index < len(statement):
        if statement.startswith("/*", index):
            depth = 1
            index += 2
            while index < len(statement) and depth:
                if statement.startswith("/*", index):
                    depth += 1
                    index += 2
                elif statement.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            parts.append(" ")
        elif statement.startswith("--", index):
            while index < len(statement) and statement[index] not in "\r\n":
                index += 1
            parts.append(" ")
        else:
            quoted = next(
                (
                    match
                    for pattern in (_STRING_LITERAL, _TAGGED_DOLLAR_STRING, _UNTAGGED_DOLLAR_STRING)
                    if (match := pattern.match(statement, index))
                ),
                None,
            )
            if quoted:
                parts.append(quoted.group())
                index = quoted.end()
            else:
                parts.append(statement[index])
                index += 1
    return "".join(parts)


def sql_fingerprint(statement: str) -> str:
    """Normalize SQL structure without retaining literals, comments or bind values."""
    value = _TAGGED_DOLLAR_STRING.sub("?", _without_comments(str(statement)))
    value = _UNTAGGED_DOLLAR_STRING.sub("?", value)
    value = _HEX_LITERAL.sub("?", value)
    value = _STRING_LITERAL.sub("?", value)
    value = _UUID_LITERAL.sub("?", value)
    value = _HEX_NUMBER.sub("?", value)
    value = _NUMBER_LITERAL.sub("?", value)
    value = _NAMED_BIND.sub("?", value)
    return _WHITESPACE.sub(" ", value).strip()
