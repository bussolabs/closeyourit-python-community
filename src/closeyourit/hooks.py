"""Fail-safe integrations with Python logging and unhandled exception hooks."""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
from collections.abc import Callable, Mapping
from contextvars import ContextVar
from types import TracebackType
from typing import Any

from closeyourit._diagnostics import LOGGER_NAME as _INTERNAL_LOGGER
from closeyourit.client import Client

SysExceptionHook = Callable[[type[BaseException], BaseException, TracebackType | None], Any]
ThreadExceptionHook = Callable[[threading.ExceptHookArgs], Any]
LoopExceptionHandler = Callable[[asyncio.AbstractEventLoop, dict[str, Any]], Any]

_CAPTURING: ContextVar[bool] = ContextVar("closeyourit_hook_capturing", default=False)
_LOG_RECORD_FIELDS = frozenset(
    logging.LogRecord(
        name="",
        level=0,
        pathname="",
        lineno=0,
        msg="",
        args=(),
        exc_info=None,
    ).__dict__
) | {"message", "asctime"}


def _is_internal_logger(name: str) -> bool:
    return name == _INTERNAL_LOGGER or name.startswith(f"{_INTERNAL_LOGGER}.")


def _level_name(level: int) -> str:
    if level >= logging.CRITICAL:
        return "fatal"
    if level >= logging.ERROR:
        return "error"
    if level >= logging.WARNING:
        return "warning"
    if level >= logging.INFO:
        return "info"
    return "debug"


def _is_cancellation(error: BaseException) -> bool:
    return isinstance(
        error,
        (asyncio.CancelledError, GeneratorExit, KeyboardInterrupt, SystemExit),
    )


class CloseYourItHandler(logging.Handler):
    """Forward standard-library log records without ever disrupting the application."""

    def __init__(self, client: Client, level: int | str = logging.WARNING) -> None:
        super().__init__(level=level)
        self.client = client
        self._installed_loggers: set[logging.Logger] = set()
        self._closeyourit_closed = False

    def emit(self, record: logging.LogRecord) -> None:
        if (
            self._closeyourit_closed
            or record.levelno < self.level
            or _is_internal_logger(record.name)
            or _CAPTURING.get()
        ):
            return

        token = _CAPTURING.set(True)
        try:
            attributes = {
                key: value
                for key, value in record.__dict__.items()
                if key not in _LOG_RECORD_FIELDS and not key.startswith("_")
            }
            self.client.log(
                record.getMessage(),
                level=_level_name(record.levelno),
                logger=record.name,
                **attributes,
            )
        except BaseException:
            # Observability must never alter application logging control flow.
            return
        finally:
            _CAPTURING.reset(token)

    def install(self, logger: logging.Logger | None = None) -> CloseYourItHandler:
        """Attach once to a logger (the root logger by default)."""
        if self._closeyourit_closed:
            return self
        target = logging.getLogger() if logger is None else logger
        if self not in target.handlers:
            target.addHandler(self)
        self._installed_loggers.add(target)
        return self

    def uninstall(self) -> None:
        """Detach from every logger previously installed on."""
        for logger in tuple(self._installed_loggers):
            if self in logger.handlers:
                logger.removeHandler(self)
        self._installed_loggers.clear()

    def close(self) -> None:
        if self._closeyourit_closed:
            return
        self._closeyourit_closed = True
        self.uninstall()
        super().close()


class ErrorHooks:
    """Install reversible sys, threading and asyncio unhandled-error hooks."""

    def __init__(self, client: Client) -> None:
        self.client = client
        self._previous_sys: SysExceptionHook | None = None
        self._previous_thread: ThreadExceptionHook | None = None
        self._sys_hook: SysExceptionHook = self._handle_sys
        self._thread_hook: ThreadExceptionHook = self._handle_thread
        self._loops: dict[
            asyncio.AbstractEventLoop,
            tuple[LoopExceptionHandler | None, LoopExceptionHandler],
        ] = {}
        self._installed = False
        self._closed = False
        self._close_result = True

    @property
    def sys_hook(self) -> SysExceptionHook:
        return self._sys_hook

    @property
    def thread_hook(self) -> ThreadExceptionHook:
        return self._thread_hook

    def install(self, loop: asyncio.AbstractEventLoop | None = None) -> ErrorHooks:
        """Install global hooks once and optionally instrument one or more event loops."""
        if self._closed:
            return self
        if not self._installed:
            self._previous_sys = sys.excepthook
            self._previous_thread = threading.excepthook
            sys.excepthook = self._sys_hook
            threading.excepthook = self._thread_hook
            self._installed = True
        if loop is not None and loop not in self._loops:
            previous = loop.get_exception_handler()

            def handler(
                active_loop: asyncio.AbstractEventLoop,
                context: dict[str, Any],
                *,
                previous_handler: LoopExceptionHandler | None = previous,
            ) -> None:
                self._handle_asyncio(active_loop, context, previous_handler)

            self._loops[loop] = (previous, handler)
            loop.set_exception_handler(handler)
        return self

    def uninstall(self) -> None:
        """Restore only hooks still owned by this instance."""
        if self._installed:
            if sys.excepthook is self._sys_hook and self._previous_sys is not None:
                sys.excepthook = self._previous_sys
            if threading.excepthook is self._thread_hook and self._previous_thread is not None:
                threading.excepthook = self._previous_thread
            self._installed = False

        for loop, (previous, handler) in tuple(self._loops.items()):
            try:
                if not loop.is_closed() and loop.get_exception_handler() is handler:
                    loop.set_exception_handler(previous)
            except BaseException:
                pass
        self._loops.clear()

    def close(self, timeout: float | None = None) -> bool:
        """Uninstall all hooks and close the client exactly once."""
        if self._closed:
            return self._close_result
        self._closed = True
        self.uninstall()
        try:
            self._close_result = self.client.close(timeout)
        except BaseException:
            self._close_result = False
        return self._close_result

    def _handle_sys(
        self,
        exc_type: type[BaseException],
        error: BaseException,
        traceback: TracebackType | None,
    ) -> None:
        self._capture_exception(error, contexts={"mechanism": "sys.excepthook"})
        previous = self._previous_sys
        if previous is not None and previous is not self._sys_hook:
            previous(exc_type, error, traceback)

    def _handle_thread(self, args: threading.ExceptHookArgs) -> None:
        error = args.exc_value
        if error is not None:
            contexts: dict[str, object] = {"mechanism": "threading.excepthook"}
            if args.thread is not None:
                contexts["thread"] = args.thread.name
            self._capture_exception(error, contexts=contexts)
        previous = self._previous_thread
        if previous is not None and previous is not self._thread_hook:
            previous(args)

    def _handle_asyncio(
        self,
        loop: asyncio.AbstractEventLoop,
        context: dict[str, Any],
        previous: LoopExceptionHandler | None,
    ) -> None:
        error = context.get("exception")
        future = context.get("future", context.get("task"))
        cancelled_method = getattr(future, "cancelled", None)
        cancelled = callable(cancelled_method) and bool(cancelled_method())
        if not cancelled:
            message = str(context.get("message") or "Unhandled asyncio exception")
            if isinstance(error, BaseException):
                self._capture_exception(
                    error,
                    contexts={"mechanism": "asyncio", "message": message},
                )
            else:
                self._capture_log(message, mechanism="asyncio")

        if previous is None:
            loop.default_exception_handler(context)
        else:
            previous(loop, context)

    def _capture_exception(self, error: BaseException, *, contexts: Mapping[str, object]) -> None:
        if _is_cancellation(error) or _CAPTURING.get():
            return
        token = _CAPTURING.set(True)
        try:
            self.client.capture_exception(
                error,
                handled=False,
                level="fatal",
                contexts=contexts,
            )
        except BaseException:
            pass
        finally:
            _CAPTURING.reset(token)

    def _capture_log(self, message: str, *, mechanism: str) -> None:
        if _CAPTURING.get():
            return
        token = _CAPTURING.set(True)
        try:
            self.client.log(
                message,
                level="fatal",
                logger=mechanism,
                mechanism=mechanism,
            )
        except BaseException:
            pass
        finally:
            _CAPTURING.reset(token)
