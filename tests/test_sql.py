from __future__ import annotations

from closeyourit.sql import sql_fingerprint


def test_sql_fingerprint_redacts_postgresql_dollar_quoted_strings_without_touching_binds() -> None:
    fingerprint = sql_fingerprint("SELECT $1, $$private secret$$, $audit$token=hidden$audit$, $2")

    assert fingerprint == "SELECT $1, ?, ?, $2"
    assert "private secret" not in fingerprint
    assert "token=hidden" not in fingerprint


def test_sql_fingerprint_redacts_backslash_escaped_single_quoted_strings() -> None:
    fingerprint = sql_fingerprint("SELECT 'private\\\\path', 'secret\\'suffix', $1")

    assert fingerprint == "SELECT ?, ?, $1"
    assert "private" not in fingerprint
    assert "secret" not in fingerprint
    assert "suffix" not in fingerprint


def test_nested_comments_are_removed_in_full() -> None:
    assert sql_fingerprint("SELECT /* outer /* inner */ PrivateMarker */ 1") == "SELECT ?"
    assert sql_fingerprint("SELECT /* outer /* inner */ PrivateMarker") == "SELECT"


def test_quotes_inside_comments_cannot_hide_the_comment_boundary() -> None:
    assert sql_fingerprint("SELECT /* outer ' /* inner */ PrivateMarker */ 'value'") == "SELECT ?"


def test_comment_markers_inside_strings_remain_literals() -> None:
    assert sql_fingerprint("SELECT '/* not a comment */', $$-- value$$, 1") == "SELECT ?, ?, ?"
