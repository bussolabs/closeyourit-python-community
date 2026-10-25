from __future__ import annotations

import asyncio
import logging
import sys
import threading
from types import SimpleNamespace
from typing import cast

import pytest

from closeyourit import Client, CloseYourItHandler, Configuration, ErrorHooks
from closeyourit.client import Channel


class RecordingSink:
    def __init__(self) -> None:
        self.items: list[tuple[Channel, dict[str, object]]] = []

    def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
        self.items.append((channel, payload))
        return True


class RecordingClient:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.exceptions: list[tuple[BaseException, bool, str, dict[str, object] | None]] = []
        self.logs: list[tuple[object, str, str | None, dict[str, object]]] = []
        self.close_calls: list[float | None] = []

    def capture_exception(
        self,
        error: BaseException,
        *,
        handled: bool = False,
        level: str = "error",
        contexts: dict[str, object] | None = None,
    ) -> str | None:
        if self.fail:
            raise RuntimeError("capture failed")
        self.exceptions.append((error, handled, level, contexts))
        return "event-id"

    def log(
        self,
        message: object,
        *,
        level: str = "info",
        logger: str | None = None,
        **attributes: object,
    ) -> str | None:
        if self.fail:
            raise RuntimeError("log failed")
        self.logs.append((message, level, logger, attributes))
        return "log-id"

    def close(self, timeout: float | None = None) -> bool:
        self.close_calls.append(timeout)
        return True


def valid_configuration() -> Configuration:
    return Configuration(
        endpoint_url="https://www.closeyour.it",
        token="cyi_server-token",
        project_id="12345678-1234-1234-1234-123456789abc",
        environment="production",
    )


def isolated_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    return logger


def test_logging_handler_applies_min_level_and_scrubs_extra_attributes() -> None:
    sink = RecordingSink()
    client = Client(valid_configuration(), sink=sink)
    logger = isolated_logger("payments")
    handler = CloseYourItHandler(client, level=logging.WARNING)
    logger.addHandler(handler)

    logger.info("ignored", extra={"order_id": 1})
    logger.warning("retry %s", 2, extra={"order_id": 7, "api_token": "secret"})

    assert len(sink.items) == 1
    channel, payload = sink.items[0]
    assert channel == "logs"
    assert payload["message"] == "retry 2"
    assert payload["level"] == "warning"
    assert payload["logger"] == "payments"
    assert payload["attributes"] == {"order_id": 7, "api_token": "[FILTERED]"}


def test_logging_handler_excludes_the_internal_logger_namespace() -> None:
    client = RecordingClient()
    handler = CloseYourItHandler(cast(Client, client), level=logging.DEBUG)
    internal_root = isolated_logger("closeyourit")
    internal = isolated_logger("closeyourit.transport")
    similarly_named = isolated_logger("closeyourit_app")
    internal_root.addHandler(handler)
    internal.addHandler(handler)
    similarly_named.addHandler(handler)

    internal_root.error("internal root failure")
    internal.error("internal failure")
    similarly_named.error("application failure")

    assert [item[0] for item in client.logs] == ["application failure"]


@pytest.mark.parametrize(
    ("log_level", "expected"),
    [
        (logging.DEBUG, "debug"),
        (logging.INFO, "info"),
        (logging.ERROR, "error"),
        (logging.CRITICAL, "fatal"),
    ],
)
def test_logging_handler_maps_standard_levels(log_level: int, expected: str) -> None:
    client = RecordingClient()
    logger = isolated_logger(f"level-{expected}")
    logger.addHandler(CloseYourItHandler(cast(Client, client), level=logging.DEBUG))

    logger.log(log_level, "message")

    assert client.logs[0][1] == expected


def test_logging_handler_is_fail_safe_and_guards_against_recursion() -> None:
    logger = isolated_logger("recursive")

    class RecursiveClient(RecordingClient):
        def log(
            self,
            message: object,
            *,
            level: str = "info",
            logger: str | None = None,
            **attributes: object,
        ) -> str | None:
            result = super().log(message, level=level, logger=logger, **attributes)
            logging.getLogger("recursive").warning("nested")
            return result

    recursive = RecursiveClient()
    handler = CloseYourItHandler(cast(Client, recursive), level=logging.DEBUG)
    logger.addHandler(handler)

    logger.warning("outer")

    assert [item[0] for item in recursive.logs] == ["outer"]

    logger.handlers.clear()
    logger.addHandler(CloseYourItHandler(cast(Client, RecordingClient(fail=True))))
    logger.error("must not escape")


def test_logging_handler_install_uninstall_and_close_are_idempotent() -> None:
    logger = isolated_logger("lifecycle")
    handler = CloseYourItHandler(cast(Client, RecordingClient()))

    assert handler.install(logger) is handler
    assert handler.install(logger) is handler
    assert logger.handlers.count(handler) == 1

    handler.uninstall()
    handler.uninstall()
    assert handler not in logger.handlers

    handler.install(logger)
    handler.close()
    handler.close()
    assert handler not in logger.handlers


def test_sys_hook_captures_fatal_unhandled_and_chains_previous_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = RecordingClient()
    previous_calls: list[tuple[type[BaseException], BaseException, object]] = []

    def previous(exc_type: type[BaseException], error: BaseException, traceback: object) -> None:
        previous_calls.append((exc_type, error, traceback))

    monkeypatch.setattr(sys, "excepthook", previous)
    hooks = ErrorHooks(cast(Client, client))
    error = RuntimeError("boom")

    hooks.install()
    hooks.install()
    sys.excepthook(type(error), error, error.__traceback__)

    assert client.exceptions == [(error, False, "fatal", {"mechanism": "sys.excepthook"})]
    assert previous_calls == [(RuntimeError, error, None)]

    hooks.uninstall()
    hooks.uninstall()
    assert sys.excepthook is previous


def test_sys_hook_chains_previous_even_when_capture_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_errors: list[BaseException] = []
    monkeypatch.setattr(
        sys, "excepthook", lambda _type, error, _traceback: previous_errors.append(error)
    )
    hooks = ErrorHooks(cast(Client, RecordingClient(fail=True)))
    error = RuntimeError("boom")

    hooks.install()
    sys.excepthook(type(error), error, None)

    assert previous_errors == [error]
    hooks.uninstall()


def test_thread_hook_captures_context_chains_and_skips_system_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = RecordingClient()
    previous_args: list[threading.ExceptHookArgs] = []
    monkeypatch.setattr(threading, "excepthook", previous_args.append)
    hooks = ErrorHooks(cast(Client, client))
    worker = threading.Thread(name="worker-1")
    error = ValueError("thread failed")
    args = cast(
        threading.ExceptHookArgs,
        SimpleNamespace(
            exc_type=ValueError,
            exc_value=error,
            exc_traceback=None,
            thread=worker,
        ),
    )
    stopped = SystemExit(0)
    stopped_args = cast(
        threading.ExceptHookArgs,
        SimpleNamespace(
            exc_type=SystemExit,
            exc_value=stopped,
            exc_traceback=None,
            thread=worker,
        ),
    )

    hooks.install()
    threading.excepthook(args)
    threading.excepthook(stopped_args)

    assert client.exceptions == [
        (error, False, "fatal", {"mechanism": "threading.excepthook", "thread": "worker-1"})
    ]
    assert previous_args == [args, stopped_args]
    hooks.uninstall()


def test_asyncio_hook_captures_once_chains_previous_and_skips_cancellation() -> None:
    client = RecordingClient()
    hooks = ErrorHooks(cast(Client, client))
    loop = asyncio.new_event_loop()
    previous_contexts: list[dict[str, object]] = []
    loop.set_exception_handler(lambda _loop, context: previous_contexts.append(context))
    error = LookupError("task failed")
    failed_context: dict[str, object] = {"message": "Task exception", "exception": error}
    cancelled_context: dict[str, object] = {
        "message": "Task cancelled",
        "exception": asyncio.CancelledError(),
    }

    try:
        hooks.install(loop)
        installed = loop.get_exception_handler()
        hooks.install(loop)
        assert loop.get_exception_handler() is installed

        loop.call_exception_handler(failed_context)
        loop.call_exception_handler(cancelled_context)

        assert client.exceptions == [
            (
                error,
                False,
                "fatal",
                {"mechanism": "asyncio", "message": "Task exception"},
            )
        ]
        assert previous_contexts == [failed_context, cancelled_context]

        hooks.uninstall()
        assert loop.get_exception_handler() is not installed
    finally:
        loop.close()


def test_asyncio_hook_captures_message_and_calls_default_handler_without_previous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = RecordingClient()
    hooks = ErrorHooks(cast(Client, client))
    loop = asyncio.new_event_loop()
    default_contexts: list[dict[str, object]] = []
    monkeypatch.setattr(loop, "default_exception_handler", default_contexts.append)
    context: dict[str, object] = {"message": "orphaned task"}

    try:
        hooks.install(loop)
        loop.call_exception_handler(context)

        assert client.logs == [
            (
                "orphaned task",
                "fatal",
                "asyncio",
                {"mechanism": "asyncio"},
            )
        ]
        assert default_contexts == [context]
    finally:
        hooks.uninstall()
        loop.close()


def test_asyncio_hook_skips_cancelled_future() -> None:
    client = RecordingClient()
    hooks = ErrorHooks(cast(Client, client))
    loop = asyncio.new_event_loop()
    future = loop.create_future()
    future.cancel()

    try:
        hooks.install(loop)
        loop.call_exception_handler({"message": "cancelled", "future": future})

        assert client.exceptions == []
        assert client.logs == []
    finally:
        hooks.uninstall()
        loop.close()


def test_hook_recursion_guard_and_close_are_fail_safe_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_errors: list[BaseException] = []
    monkeypatch.setattr(
        sys, "excepthook", lambda _type, error, _traceback: previous_errors.append(error)
    )

    class RecursiveClient(RecordingClient):
        def capture_exception(
            self,
            error: BaseException,
            *,
            handled: bool = False,
            level: str = "error",
            contexts: dict[str, object] | None = None,
        ) -> str | None:
            result = super().capture_exception(
                error, handled=handled, level=level, contexts=contexts
            )
            sys.excepthook(type(error), error, error.__traceback__)
            return result

    client = RecursiveClient()
    hooks = ErrorHooks(cast(Client, client))
    error = RuntimeError("recursive")
    hooks.install()

    sys.excepthook(type(error), error, None)

    assert len(client.exceptions) == 1
    assert previous_errors == [error, error]
    assert hooks.close(0.5) is True
    assert hooks.close(1.0) is True
    assert client.close_calls == [0.5]
    assert sys.excepthook is not hooks.sys_hook
    assert hooks.install() is hooks


def test_uninstall_preserves_hooks_replaced_by_the_application(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hooks = ErrorHooks(cast(Client, RecordingClient()))

    def replacement_sys(
        _type: type[BaseException], _error: BaseException, _traceback: object
    ) -> None:
        pass

    def replacement_thread(_args: threading.ExceptHookArgs) -> None:
        pass

    hooks.install()
    monkeypatch.setattr(sys, "excepthook", replacement_sys)
    monkeypatch.setattr(threading, "excepthook", replacement_thread)
    hooks.uninstall()

    assert sys.excepthook is replacement_sys
    assert threading.excepthook is replacement_thread


def test_close_is_fail_safe_when_the_client_close_fails() -> None:
    class FailingCloseClient(RecordingClient):
        def close(self, timeout: float | None = None) -> bool:
            raise RuntimeError("close failed")

    hooks = ErrorHooks(cast(Client, FailingCloseClient()))

    assert hooks.close() is False
    assert hooks.close() is False
