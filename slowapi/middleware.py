import inspect
from typing import Callable, Iterable, Optional, Tuple

from starlette.applications import Starlette
from starlette.datastructures import MutableHeaders
from starlette.middleware.base import (
    BaseHTTPMiddleware,
    RequestResponseEndpoint,
)
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import BaseRoute, Match
from starlette.types import ASGIApp, Message, Scope, Receive, Send

from slowapi import Limiter, _rate_limit_exceeded_handler


def _find_route_handler(
    routes: Iterable[BaseRoute], scope: Scope
) -> Optional[Callable]:
    pass


def _get_route_name(handler: Callable):
    pass


def _check_limits(
    limiter: Limiter, request: Request, handler: Optional[Callable], app: Starlette
) -> Tuple[Optional[Callable], bool, Optional[Exception]]:
    """
    Utils to check (if needed) current requests limit.
    It returns a tuple of size 3:
        1. The exception handler to run, if needed
        2. a bool, True if we need to inject some headers, False otherwise
        3. the exception that happened, if any
    """
    pass


def sync_check_limits(
    limiter: Limiter, request: Request, handler: Optional[Callable], app: Starlette
) -> Tuple[Optional[Response], bool]:
    """
    Returns a `Response` object if an error occurred, as well as a boolean to know
    whether we should inject headers or not.
    Used in our WSGI middleware, it only supports synchronous exception_handler.
    This will fallback on _rate_limit_exceeded_handler otherwise.
    """
    pass


async def async_check_limits(
    limiter: Limiter, request: Request, handler: Optional[Callable], app: Starlette
) -> Tuple[Optional[Response], bool]:
    """
    Returns a `Response` object if an error occurred, as well as a boolean to know
    whether we should inject headers or not.
    Used in our ASGI middleware, this support both synchronous or asynchronous exception handlers.
    """
    pass


def _should_exempt(limiter: Limiter, handler: Optional[Callable]) -> bool:
    # if we can't find the route handler
    pass


class SlowAPIMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        pass


class SlowAPIASGIMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        await _ASGIMiddlewareResponder(self.app)(scope, receive, send)


class _ASGIMiddlewareResponder:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app
        self.error_response: Optional[Response] = None
        self.initial_message: Message = {}
        self.inject_headers = False

    async def send_wrapper(self, message: Message) -> None:
        pass

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        self.send = send

        _app: Starlette = scope["app"]
        limiter: Limiter = _app.state.limiter

        if not limiter.enabled:
            return await self.app(scope, receive, self.send)

        handler = _find_route_handler(_app.routes, scope)
        request = Request(scope, receive=receive, send=self.send)
        if _should_exempt(limiter, handler):
            return await self.app(scope, receive, self.send)

        error_response, should_inject_headers = await async_check_limits(
            limiter, request, handler, _app
        )
        if error_response is not None:
            return await error_response(scope, receive, self.send_wrapper)

        if should_inject_headers:
            self.inject_headers = True
            self.limiter = limiter
            self.request = request

        return await self.app(scope, receive, self.send_wrapper)
