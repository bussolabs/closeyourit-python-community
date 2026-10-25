"""Bounded, fork-safe asynchronous HTTP transport using only the standard library."""

from __future__ import annotations

import atexit
import json
import os
import queue
import random
import threading
import time
import weakref
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from typing import Literal, Protocol
from urllib.error import HTTPError
from urllib.parse import quote, urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, OpenerDirector, Request, build_opener

from closeyourit._diagnostics import logger
from closeyourit._version import __version__
from closeyourit.configuration import Configuration
from closeyourit.models import EventPayload

Channel = Literal["events", "logs", "metrics", "usages"]
_CHANNELS = frozenset({"events", "logs", "metrics", "usages"})
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_RETRY_STATUSES = frozenset({408, 425, 429})
_DEFAULT_LOSS_LOG_INTERVAL = 100


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """Small HTTP result detached from the standard-library response object."""

    status: int
    headers: Mapping[str, str] = field(default_factory=dict)
    body: bytes = b""


class HttpRequester(Protocol):
    def send(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse: ...


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(  # type: ignore[override]
        self,
        req: Request,
        fp: object,
        code: int,
        msg: str,
        headers: Mapping[str, str],
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


class UrllibHttpRequester:
    """Execute one POST without implicit redirects, preserving control of credentials."""

    def __init__(self, opener: OpenerDirector | None = None) -> None:
        self._opener = opener or build_opener(_NoRedirectHandler())

    def send(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        request = Request(url, data=body, headers=dict(headers), method="POST")
        try:
            with self._opener.open(request, timeout=timeout) as response:
                return HttpResponse(
                    status=response.getcode(),
                    headers=dict(response.headers.items()),
                    body=response.read(65_536),
                )
        except HTTPError as error:
            try:
                response_body = error.read(65_536)
            finally:
                error.close()
            return HttpResponse(
                status=error.code,
                headers=dict(error.headers.items()) if error.headers else {},
                body=response_body,
            )


@dataclass(frozen=True, slots=True)
class TransportStats:
    """Thread-safe diagnostic snapshot for accepted and terminal event states."""

    enqueued: int
    dropped: int
    sent: int
    failed: int
    retried: int
    pending: int
    queue_size: int
    worker_alive: bool
    closed: bool
    pid: int
    last_failure: str | None


@dataclass(frozen=True, slots=True)
class _Envelope:
    channel: Channel
    body: bytes


_FORK_REGISTRY: weakref.WeakSet[Transport] = weakref.WeakSet()
_FORK_REGISTRY_LOCK = threading.Lock()
_FORK_CALLBACK_REGISTERED = False


def _reset_transports_after_fork() -> None:
    for transport in tuple(_FORK_REGISTRY):
        transport._after_fork_child()


def _register_for_fork(transport: Transport) -> None:
    global _FORK_CALLBACK_REGISTERED
    if not hasattr(os, "register_at_fork"):
        return
    with _FORK_REGISTRY_LOCK:
        _FORK_REGISTRY.add(transport)
        if not _FORK_CALLBACK_REGISTERED:
            os.register_at_fork(after_in_child=_reset_transports_after_fork)
            _FORK_CALLBACK_REGISTERED = True


class Transport:
    """Non-blocking sink with bounded memory, retry, flush and process isolation."""

    def __init__(
        self,
        configuration: Configuration,
        *,
        requester: HttpRequester | None = None,
        max_queue: int = 100,
        request_timeout: float = 3.0,
        max_retries: int = 2,
        max_redirects: int = 2,
        retry_base_delay: float = 0.1,
        max_retry_delay: float = 30.0,
        shutdown_timeout: float = 2.0,
        loss_log_interval: int = _DEFAULT_LOSS_LOG_INTERVAL,
        sleep: Callable[[float], None] = time.sleep,
        random_value: Callable[[], float] = random.random,
        monotonic: Callable[[], float] = time.monotonic,
        wall_time: Callable[[], float] = time.time,
    ) -> None:
        self.configuration = configuration
        self._requester = requester or UrllibHttpRequester()
        self._max_queue = max(1, int(max_queue))
        self._request_timeout = max(0.001, float(request_timeout))
        self._max_retries = max(0, int(max_retries))
        self._max_redirects = max(0, int(max_redirects))
        self._retry_base_delay: float = max(0.0, float(retry_base_delay))
        self._max_retry_delay: float = max(0.0, float(max_retry_delay))
        self._shutdown_timeout = max(0.0, float(shutdown_timeout))
        self._loss_log_interval = max(1, int(loss_log_interval))
        self._sleep = sleep
        self._random_value = random_value
        self._monotonic = monotonic
        self._wall_time = wall_time
        self._pid = os.getpid()
        self._initialize_process_state()
        _register_for_fork(self)

    def _initialize_process_state(self) -> None:
        self._queue: queue.Queue[_Envelope] = queue.Queue(maxsize=self._max_queue)
        self._condition = threading.Condition()
        self._worker: threading.Thread | None = None
        self._closed = False
        self._atexit_registered = False
        self._enqueued = 0
        self._dropped = 0
        self._sent = 0
        self._failed = 0
        self._retried = 0
        self._pending = 0
        self._last_failure: str | None = None

    def enqueue(self, channel: Channel, payload: EventPayload) -> bool:
        """Snapshot and enqueue without waiting for network or queue capacity."""
        self._ensure_process()
        if not self.configuration.enabled:
            self._record_drop("disabled")
            return False
        if channel not in _CHANNELS:
            self._record_drop("unknown_channel")
            return False
        try:
            body = json.dumps(
                payload,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError, OverflowError):
            self._record_drop("unserializable_payload")
            return False

        with self._condition:
            if self._closed:
                reason: str | None = "closed"
            else:
                try:
                    self._queue.put_nowait(_Envelope(channel=channel, body=body))
                except queue.Full:
                    reason = "queue_full"
                else:
                    self._enqueued += 1
                    self._pending += 1
                    reason = None
        if reason is not None:
            self._record_drop(reason)
            return False
        self._start_worker()
        return True

    @property
    def stats(self) -> TransportStats:
        self._ensure_process()
        with self._condition:
            worker = self._worker
            return TransportStats(
                enqueued=self._enqueued,
                dropped=self._dropped,
                sent=self._sent,
                failed=self._failed,
                retried=self._retried,
                pending=self._pending,
                queue_size=self._queue.qsize(),
                worker_alive=worker is not None and worker.is_alive(),
                closed=self._closed,
                pid=self._pid,
                last_failure=self._last_failure,
            )

    def flush(self, timeout: float | None = None) -> bool:
        """Wait until every accepted event reaches a terminal state."""
        self._ensure_process()
        duration = self._shutdown_timeout if timeout is None else max(0.0, float(timeout))
        deadline = self._monotonic() + duration
        with self._condition:
            while self._pending:
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def close(self, timeout: float | None = None) -> bool:
        """Stop acceptance, drain pending events and terminate the worker idempotently."""
        return self._close(timeout, allow_start=True)

    def _close(self, timeout: float | None, *, allow_start: bool) -> bool:
        self._ensure_process()
        duration = self._shutdown_timeout if timeout is None else max(0.0, float(timeout))
        deadline = self._monotonic() + duration
        with self._condition:
            self._closed = True
            pending = self._pending
        if pending and allow_start:
            self._start_worker()
        drained = self.flush(max(0.0, deadline - self._monotonic()))
        worker = self._worker
        if worker is not None and worker is not threading.current_thread():
            worker.join(max(0.0, deadline - self._monotonic()))
        stopped = worker is None or not worker.is_alive()
        if self._atexit_registered:
            atexit.unregister(self._atexit_close)
            self._atexit_registered = False
        return drained and stopped

    def _record_drop(self, reason: str) -> None:
        with self._condition:
            self._dropped += 1
            dropped = self._dropped
        self._log_loss("dropped", dropped, reason)

    def _log_loss(self, kind: str, count: int, reason: str) -> None:
        # First loss breaks the silence; then one line every interval keeps a busy log readable
        # while still carrying the running total. Emitted outside the lock so a consumer handler
        # can never deadlock the transport, and on the "closeyourit" logger the SDK filters out.
        if count == 1 or count % self._loss_log_interval == 0:
            logger.warning("closeyourit %s %d event(s), latest reason %s", kind, count, reason)

    def _start_worker(self) -> None:
        with self._condition:
            worker = self._worker
            if worker is not None and worker.is_alive():
                return
            if self._pending == 0:
                return
            worker = threading.Thread(
                target=self._run,
                name="closeyourit-transport",
                daemon=True,
            )
            self._worker = worker
            if not self._atexit_registered:
                atexit.register(self._atexit_close)
                self._atexit_registered = True
            worker.start()

    def _run(self) -> None:
        while True:
            with self._condition:
                if self._closed and self._pending == 0:
                    return
            try:
                envelope = self._queue.get(timeout=0.05)
            except queue.Empty:
                continue
            try:
                self._deliver(envelope)
            finally:
                self._queue.task_done()
                with self._condition:
                    self._pending -= 1
                    self._condition.notify_all()

    def _deliver(self, envelope: _Envelope) -> None:
        url = self._channel_url(envelope.channel)
        last_failure = "unknown"
        for attempt in range(self._max_retries + 1):
            try:
                response = self._post_with_redirects(url, envelope.body)
            except Exception as error:
                last_failure = f"network_error:{type(error).__name__}"
                retryable = True
                response = None
            else:
                assert response is not None
                if 200 <= response.status < 300:
                    with self._condition:
                        self._sent += 1
                    return
                last_failure = self._failure_from_response(response)
                retryable = response.status in _RETRY_STATUSES or response.status >= 500

            if not retryable or attempt >= self._max_retries:
                break
            delay = self._retry_delay(response, attempt)
            with self._condition:
                self._retried += 1
            if delay:
                self._sleep(delay)

        with self._condition:
            self._failed += 1
            self._last_failure = last_failure
            failed = self._failed
        self._log_loss("failed", failed, last_failure)

    def _post_with_redirects(self, origin_url: str, body: bytes) -> HttpResponse:
        current_url = origin_url
        for redirect_count in range(self._max_redirects + 1):
            headers = {
                "Content-Type": "application/json",
                "User-Agent": f"closeyourit-python/{__version__}",
            }
            if self._same_authority(origin_url, current_url):
                headers["Authorization"] = f"Bearer {self.configuration.token}"
            response = self._requester.send(current_url, body, headers, self._request_timeout)
            location = self._header(response.headers, "location")
            if response.status not in _REDIRECT_STATUSES or not location:
                return response
            if redirect_count >= self._max_redirects:
                return response
            target = urljoin(current_url, location)
            if urlsplit(target).scheme not in {"http", "https"}:
                return response
            current_url = target
        return response

    def _channel_url(self, channel: Channel) -> str:
        endpoint = self.configuration.endpoint_url or ""
        project_id = quote(self.configuration.project_id or "", safe="")
        return f"{endpoint}/api/v1/projects/{project_id}/{channel}"

    @staticmethod
    def _header(headers: Mapping[str, str], name: str) -> str | None:
        expected = name.casefold()
        return next((value for key, value in headers.items() if key.casefold() == expected), None)

    @staticmethod
    def _same_authority(origin: str, target: str) -> bool:
        left = urlsplit(origin)
        right = urlsplit(target)

        def authority(scheme: str, hostname: str | None, port: int | None) -> tuple[str, str, int]:
            default_port = 443 if scheme == "https" else 80
            return scheme, (hostname or "").casefold(), port or default_port

        return authority(left.scheme, left.hostname, left.port) == authority(
            right.scheme, right.hostname, right.port
        )

    def _retry_delay(self, response: HttpResponse | None, attempt: int) -> float:
        if response is not None:
            retry_after = self._header(response.headers, "retry-after")
            parsed = self._parse_retry_after(retry_after)
            if parsed is not None:
                return min(self._max_retry_delay, parsed)
        exponential = self._retry_base_delay * float(2**attempt)
        base = min(self._max_retry_delay, exponential)
        jitter = float(self._random_value())
        delay = float(base + base * jitter)
        return min(self._max_retry_delay, delay)

    def _parse_retry_after(self, value: str | None) -> float | None:
        if value is None:
            return None
        try:
            return max(0.0, float(value.strip()))
        except ValueError:
            try:
                parsed = parsedate_to_datetime(value)
                parsed_timestamp = float(parsed.timestamp())
                return max(0.0, parsed_timestamp - self._wall_time())
            except (TypeError, ValueError, OverflowError):
                return None

    @staticmethod
    def _failure_from_response(response: HttpResponse) -> str:
        suffix = ""
        try:
            payload = json.loads(response.body)
            error = payload.get("error") if isinstance(payload, dict) else None
            code = error.get("code") if isinstance(error, dict) else None
            if isinstance(code, str) and code:
                suffix = f":{code}"
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        return f"http_{response.status}{suffix}"

    def _ensure_process(self) -> None:
        if os.getpid() != self._pid:
            self._after_fork_child()

    def _after_fork_child(self) -> None:
        inherited_atexit_registration = self._atexit_registered
        self._pid = os.getpid()
        self._initialize_process_state()
        self._atexit_registered = inherited_atexit_registration

    def _atexit_close(self) -> None:
        # Python 3.12+ rejects starting threads during interpreter shutdown. Every accepted
        # envelope already has a worker, so the exit hook only drains an existing one.
        self._close(self._shutdown_timeout, allow_start=False)
