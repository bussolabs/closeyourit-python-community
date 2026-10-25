"""Explicit optional FastAPI entry point using the shared Starlette lifecycle."""

from __future__ import annotations

from typing import TYPE_CHECKING

from closeyourit.client import Client
from closeyourit.starlette import StarletteIntegration, instrument_starlette

if TYPE_CHECKING:
    from fastapi import FastAPI


def instrument_fastapi(
    app: FastAPI, client: Client, *, capture_handled_errors: bool | None = None
) -> StarletteIntegration:
    from fastapi import FastAPI

    if not isinstance(app, FastAPI):
        raise TypeError("Expected a FastAPI application")
    return instrument_starlette(app, client, capture_handled_errors=capture_handled_errors)
