"""Optional Django middleware with no framework import at module import time."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Iterator
from contextlib import contextmanager
from contextvars import Context, copy_context
from typing import Any, cast

from closeyourit.client import Client
from closeyourit.integrations.django import DjangoQueryProfile, instrument_django
from closeyourit.scope import with_scope
from closeyourit.web import Monotonic, RequestLifecycle, wsgi_request

_LIFECYCLE_ATTRIBUTE = "_closeyourit_lifecycle"


class DjangoMiddleware:
    """Django sync/async middleware preserving framework response semantics."""

    sync_capable = True
    async_capable = True

    def __init__(
        self,
        get_response: Callable[[object], object],
        *,
        client: Client | None = None,
        monotonic: Monotonic = time.monotonic,
        query_monitoring: bool = False,
    ) -> None:
        self.get_response = get_response
        self.client = client or Client()
        self.monotonic = monotonic
        self.queries = instrument_django(self.client) if query_monitoring else None
        try:
            from asgiref.sync import iscoroutinefunction, markcoroutinefunction
        except ImportError:
            self._is_async = inspect.iscoroutinefunction(get_response)
        else:
            self._is_async = iscoroutinefunction(get_response)
            if self._is_async:
                markcoroutinefunction(self)

    def __call__(self, request: object) -> object | Awaitable[object]:
        if self._is_async:
            return self._acall(request)
        return self._call(request)

    def _call(self, request: object) -> object:
        deferred = False
        with with_scope(), self._query_profile(request) as query_profile:
            lifecycle = self._lifecycle(request)
            lifecycle.begin()
            setattr(request, _LIFECYCLE_ATTRIBUTE, lifecycle)
            try:
                response = self.get_response(request)
                lifecycle.set_status(getattr(response, "status_code", None))
                deferred = self._defer_streaming(response, lifecycle, request, query_profile)
                return response
            except Exception as error:
                lifecycle.finish(error)
                raise
            finally:
                if not deferred:
                    lifecycle.finish()
                    self._cleanup_request(request)

    async def _acall(self, request: object) -> object:
        if self.queries is not None:
            await self.queries.attach_async()
        deferred = False
        with with_scope(), self._query_profile(request) as query_profile:
            lifecycle = self._lifecycle(request)
            lifecycle.begin()
            setattr(request, _LIFECYCLE_ATTRIBUTE, lifecycle)
            try:
                result = self.get_response(request)
                response = await cast(Awaitable[object], result)
                lifecycle.set_status(getattr(response, "status_code", None))
                deferred = self._defer_streaming(response, lifecycle, request, query_profile)
                return response
            except Exception as error:
                lifecycle.finish(error)
                raise
            finally:
                if not deferred:
                    lifecycle.finish()
                    self._cleanup_request(request)

    def process_view(
        self,
        request: object,
        view_func: Callable[..., object],
        view_args: tuple[object, ...],
        view_kwargs: dict[str, object],
    ) -> None:
        del view_args, view_kwargs
        lifecycle = self._current_lifecycle(request)
        if lifecycle is None:
            return
        route = getattr(view_func, "closeyourit_route", None)
        resolver_match = getattr(request, "resolver_match", None)
        route = getattr(resolver_match, "route", None) or route
        lifecycle.set_route(str(route) if route else None)

    def process_exception(self, request: object, exception: BaseException) -> None:
        lifecycle = self._current_lifecycle(request)
        if lifecycle is not None:
            lifecycle.set_exception(exception, handled=False)

    def _lifecycle(self, request: object) -> RequestLifecycle:
        meta = getattr(request, "META", {})
        environ = cast(dict[str, object], meta if isinstance(meta, dict) else {})
        request_data, trace_id = wsgi_request(
            environ, self.client.configuration.request_header_allowlist
        )
        path = str(getattr(request, "path", request_data.get("path", "/")))
        request_data["path"] = path
        scheme = str(getattr(request, "scheme", "http"))
        get_host = getattr(request, "get_host", None)
        try:
            host = str(get_host()) if callable(get_host) else ""
        except Exception:
            host = ""
        request_data["url"] = f"{scheme}://{host}{path}" if host else path
        return RequestLifecycle(
            self.client,
            request_data,
            trace_id,
            route_resolver=lambda: self._route_from_request(request),
            monotonic=self.monotonic,
        )

    def _defer_streaming(
        self,
        response: object,
        lifecycle: RequestLifecycle,
        request: object,
        query_profile: DjangoQueryProfile | None,
    ) -> bool:
        if not getattr(response, "streaming", False):
            return False
        streaming_response = cast(Any, response)
        content = streaming_response.streaming_content
        context = copy_context()
        if getattr(response, "is_async", False):
            streaming_response.streaming_content = self._async_stream(
                cast(AsyncIterable[object], content), context, lifecycle, request, query_profile
            )
        else:
            streaming_response.streaming_content = self._sync_stream(
                cast(Iterable[object], content), context, lifecycle, request, query_profile
            )
        if query_profile is not None:
            query_profile.deferred = True
            streaming_response._resource_closers.append(lambda: context.run(query_profile.finish))
        return True

    def _sync_stream(
        self,
        content: Iterable[object],
        context: Context,
        lifecycle: RequestLifecycle,
        request: object,
        query_profile: DjangoQueryProfile | None,
    ) -> Iterator[object]:
        iterator = iter(content)
        try:
            while True:
                try:
                    yield context.run(next, iterator)
                except StopIteration:
                    break
        except Exception as error:
            context.run(lifecycle.finish, error)
            raise
        finally:
            close = getattr(iterator, "close", None)
            try:
                if close is not None:
                    context.run(close)
            finally:
                if query_profile is not None:
                    context.run(query_profile.finish)
                context.run(lifecycle.finish)
                self._cleanup_request(request)

    async def _async_stream(
        self,
        content: AsyncIterable[object],
        context: Context,
        lifecycle: RequestLifecycle,
        request: object,
        query_profile: DjangoQueryProfile | None,
    ) -> AsyncIterator[object]:
        iterator = content.__aiter__()

        async def advance() -> object:
            return await iterator.__anext__()

        try:
            while True:
                try:
                    task = context.run(asyncio.create_task, advance())
                    yield await task
                except StopAsyncIteration:
                    break
        except Exception as error:
            context.run(lifecycle.finish, error)
            raise
        finally:
            close = getattr(iterator, "aclose", None)
            try:
                if close is not None:

                    async def close_iterator() -> None:
                        await close()

                    close_task = context.run(asyncio.create_task, close_iterator())
                    await close_task
            finally:
                if query_profile is not None:
                    context.run(query_profile.finish)
                context.run(lifecycle.finish)
                self._cleanup_request(request)

    @contextmanager
    def _query_profile(self, request: object) -> Iterator[DjangoQueryProfile | None]:
        if self.queries is None:
            yield None
        else:
            with self.queries.profile(route=lambda: self._route_from_request(request)) as profile:
                yield profile

    def uninstrument(self) -> None:
        """Remove owned query hooks without closing the shared client."""
        if self.queries is not None:
            self.queries.uninstrument()

    @staticmethod
    def _cleanup_request(request: object) -> None:
        if hasattr(request, _LIFECYCLE_ATTRIBUTE):
            delattr(request, _LIFECYCLE_ATTRIBUTE)

    @staticmethod
    def _current_lifecycle(request: object) -> RequestLifecycle | None:
        value = getattr(request, _LIFECYCLE_ATTRIBUTE, None)
        return value if isinstance(value, RequestLifecycle) else None

    @staticmethod
    def _route_from_request(request: object) -> str | None:
        resolver_match = getattr(request, "resolver_match", None)
        route = getattr(resolver_match, "route", None)
        return str(route) if route else None
