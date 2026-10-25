"""Optional, instance-local integrations with reversible instrumentation."""

from closeyourit.integrations._http import HttpTarget, normalize_http_target
from closeyourit.integrations.celery import CeleryIntegration
from closeyourit.integrations.httpx import HttpxInstrumentation, instrument_httpx
from closeyourit.integrations.requests import RequestsInstrumentation, instrument_requests
from closeyourit.integrations.sqlalchemy import (
    SQLAlchemyInstrumentation,
    instrument_sqlalchemy,
)
from closeyourit.sql import sql_fingerprint

__all__ = [
    "CeleryIntegration",
    "HttpTarget",
    "HttpxInstrumentation",
    "RequestsInstrumentation",
    "SQLAlchemyInstrumentation",
    "instrument_httpx",
    "instrument_requests",
    "instrument_sqlalchemy",
    "normalize_http_target",
    "sql_fingerprint",
]
