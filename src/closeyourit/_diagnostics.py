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
