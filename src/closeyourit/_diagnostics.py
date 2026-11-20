"""Internal diagnostic logger shared across the SDK.

Emissions here surface otherwise silent shutdowns and event loss on the host application's
standard logging, so an operator can tell "the SDK went quiet" apart from "there were no errors".
The logger name matches the one :class:`~closeyourit.hooks.CloseYourItHandler` filters out, so
these diagnostics are never re-captured and forwarded back to CloseYourIt as events. Consumers
silence them through the standard library, e.g. ``logging.getLogger("closeyourit")``.
"""

from __future__ import annotations

import logging

LOGGER_NAME = "closeyourit"
logger = logging.getLogger(LOGGER_NAME)


def warn_dropped(operation: str, error: BaseException) -> None:
    """Record why a public capture call was dropped instead of raising into the application.

    It runs inside the ``except`` blocks that keep capture calls silent, so it must never raise:
    a filter or handler the application attached to this logger may fail, and that failure is
    swallowed here without any further logging attempt that could fail the same way.
    """
    try:
        try:
            detail = str(error)
        except Exception:
            detail = "<unprintable>"
        logger.warning("closeyourit dropped %s: %s: %s", operation, type(error).__name__, detail)
    except Exception:
        pass
