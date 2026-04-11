"""
The starlette extension to rate-limit requests
"""

import asyncio
import functools
import inspect
import itertools
import logging
import os
import time
from datetime import datetime
from email.utils import formatdate, parsedate_to_datetime
from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from limits import RateLimitItem  # type: ignore
from limits.errors import ConfigurationError  # type: ignore
from limits.storage import MemoryStorage, storage_from_string  # type: ignore
from limits.strategies import STRATEGIES, RateLimiter  # type: ignore
from starlette.config import Config
from starlette.datastructures import MutableHeaders
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from typing_extensions import Literal

from .errors import RateLimitExceeded
from .wrappers import Limit, LimitGroup

# used to annotate get_app_config method
T = TypeVar("T")
# Define an alias for the most commonly used type
StrOrCallableStr = Union[str, Callable[..., str]]


class C:
    ENABLED = "RATELIMIT_ENABLED"
    HEADERS_ENABLED = "RATELIMIT_HEADERS_ENABLED"
    STORAGE_URL = "RATELIMIT_STORAGE_URL"
    STORAGE_OPTIONS = "RATELIMIT_STORAGE_OPTIONS"
    STRATEGY = "RATELIMIT_STRATEGY"
    GLOBAL_LIMITS = "RATELIMIT_GLOBAL"
    DEFAULT_LIMITS = "RATELIMIT_DEFAULT"
    APPLICATION_LIMITS = "RATELIMIT_APPLICATION"
    HEADER_LIMIT = "RATELIMIT_HEADER_LIMIT"
    HEADER_REMAINING = "RATELIMIT_HEADER_REMAINING"
    HEADER_RESET = "RATELIMIT_HEADER_RESET"
    SWALLOW_ERRORS = "RATELIMIT_SWALLOW_ERRORS"
    IN_MEMORY_FALLBACK = "RATELIMIT_IN_MEMORY_FALLBACK"
    IN_MEMORY_FALLBACK_ENABLED = "RATELIMIT_IN_MEMORY_FALLBACK_ENABLED"
    HEADER_RETRY_AFTER = "RATELIMIT_HEADER_RETRY_AFTER"
    HEADER_RETRY_AFTER_VALUE = "RATELIMIT_HEADER_RETRY_AFTER_VALUE"
    KEY_PREFIX = "RATELIMIT_KEY_PREFIX"


class HEADERS:
    RESET = 1
    REMAINING = 2
    LIMIT = 3
    RETRY_AFTER = 4


MAX_BACKEND_CHECKS = 5


def _rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> Response:
    """
    Build a simple JSON response that includes the details of the rate limit
    that was hit. If no limit is hit, the countdown is added to headers.
    """
    pass


class Limiter:
    """
    Initializes the slowapi rate limiter.

    ** parameter **

    * **app**: `Starlette/FastAPI` instance to initialize the extension
     with.

    * **default_limits**: a variable list of strings or callables returning strings denoting global
     limits to apply to all routes. `ratelimit-string` for  more details.

    * **application_limits**: a variable list of strings or callables returning strings for limits that
     are applied to the entire application (i.e a shared limit for all routes)

    * **key_func**: a callable that returns the domain to rate limit by.

    * **headers_enabled**: whether ``X-RateLimit`` response headers are written.

    * **strategy:** the strategy to use. refer to `ratelimit-strategy`

    * **storage_uri**: the storage location. refer to `ratelimit-conf`

    * **storage_options**: kwargs to pass to the storage implementation upon
      instantiation.
    * **auto_check**: whether to automatically check the rate limit in the before_request
     chain of the application. default ``True``
    * **swallow_errors**: whether to swallow errors when hitting a rate limit.
     An exception will still be logged. default ``False``
    * **in_memory_fallback**: a variable list of strings or callables returning strings denoting fallback
     limits to apply when the storage is down.
    * **in_memory_fallback_enabled**: simply falls back to in memory storage
     when the main storage is down and inherits the original limits.
    * **key_prefix**: prefix prepended to rate limiter keys.
    * **enabled**: set to False to deactivate the limiter (default: True)
    * **config_filename**: name of the config file for Starlette from which to load settings
     for the rate limiter. Defaults to ".env".
    * **key_style**: set to "url" to use the url, "endpoint" to use the view_func
    """

    def __init__(
        self,
        # app: Starlette = None,
        key_func: Callable[..., str],
        default_limits: List[StrOrCallableStr] = [],
        application_limits: List[StrOrCallableStr] = [],
        headers_enabled: bool = False,
        strategy: Optional[str] = None,
        storage_uri: Optional[str] = None,
        storage_options: Dict[str, str] = {},
        auto_check: bool = True,
        swallow_errors: bool = False,
        in_memory_fallback: List[StrOrCallableStr] = [],
        in_memory_fallback_enabled: bool = False,
        retry_after: Optional[str] = None,
        key_prefix: str = "",
        enabled: bool = True,
        config_filename: Optional[str] = None,
        key_style: Literal["endpoint", "url"] = "url",
    ) -> None:
        """
        Configure the rate limiter at app level
        """
        # assert app is not None, "Passing the app instance to the limiter is required"
        # self.app = app
        # app.state.limiter = self

        self.logger = logging.getLogger("slowapi")

        dotenv_file_exists = os.path.isfile(".env")
        self.app_config = Config(
            ".env"
            if dotenv_file_exists and config_filename is None
            else config_filename
        )

        self.enabled = enabled
        self._default_limits = []
        self._application_limits = []
        self._in_memory_fallback: List[LimitGroup] = []
        self._in_memory_fallback_enabled = (
            in_memory_fallback_enabled or len(in_memory_fallback) > 0
        )
        self._exempt_routes: Set[str] = set()
        self._request_filters: List[Callable[..., bool]] = []
        self._headers_enabled = headers_enabled
        self._header_mapping: Dict[int, str] = {}
        self._retry_after: Optional[str] = retry_after
        self._strategy = strategy
        self._storage_uri = storage_uri
        self._storage_options = storage_options
        self._auto_check = auto_check
        self._swallow_errors = swallow_errors

        self._key_func = key_func
        self._key_prefix = key_prefix
        self._key_style = key_style

        for limit in set(default_limits):
            self._default_limits.extend(
                [
                    LimitGroup(
                        limit, self._key_func, None, False, None, None, None, 1, False
                    )
                ]
            )
        for limit in application_limits:
            self._application_limits.extend(
                [
                    LimitGroup(
                        limit,
                        self._key_func,
                        "global",
                        False,
                        None,
                        None,
                        None,
                        1,
                        False,
                    )
                ]
            )
        for limit in in_memory_fallback:
            self._in_memory_fallback.extend(
                [
                    LimitGroup(
                        limit, self._key_func, None, False, None, None, None, 1, False
                    )
                ]
            )
        self._route_limits: Dict[str, List[Limit]] = {}
        self._dynamic_route_limits: Dict[str, List[LimitGroup]] = {}
        # a flag to note if the storage backend is dead (not available)
        self._storage_dead: bool = False
        self._fallback_limiter = None
        self.__check_backend_count = 0
        self.__last_check_backend = time.time()
        self.__marked_for_limiting: Dict[str, List[Callable]] = {}

        class BlackHoleHandler(logging.StreamHandler):
            def emit(*_):
                pass

        self.logger.addHandler(BlackHoleHandler())

        self.enabled = self.get_app_config(C.ENABLED, self.enabled)
        self._swallow_errors = self.get_app_config(
            C.SWALLOW_ERRORS, self._swallow_errors
        )
        self._headers_enabled = self._headers_enabled or self.get_app_config(
            C.HEADERS_ENABLED, False
        )
        self._storage_options.update(self.get_app_config(C.STORAGE_OPTIONS, {}))
        self._storage = storage_from_string(
            self._storage_uri or self.get_app_config(C.STORAGE_URL, "memory://"),
            **self._storage_options,
        )
        strategy = self._strategy or self.get_app_config(C.STRATEGY, "fixed-window")
        if strategy not in STRATEGIES:
            raise ConfigurationError("Invalid rate limiting strategy %s" % strategy)
        self._limiter: RateLimiter = STRATEGIES[strategy](self._storage)
        self._header_mapping.update(
            {
                HEADERS.RESET: self._header_mapping.get(
                    HEADERS.RESET,
                    self.get_app_config(C.HEADER_RESET, "X-RateLimit-Reset"),
                ),
                HEADERS.REMAINING: self._header_mapping.get(
                    HEADERS.REMAINING,
                    self.get_app_config(C.HEADER_REMAINING, "X-RateLimit-Remaining"),
                ),
                HEADERS.LIMIT: self._header_mapping.get(
                    HEADERS.LIMIT,
                    self.get_app_config(C.HEADER_LIMIT, "X-RateLimit-Limit"),
                ),
                HEADERS.RETRY_AFTER: self._header_mapping.get(
                    HEADERS.RETRY_AFTER,
                    self.get_app_config(C.HEADER_RETRY_AFTER, "Retry-After"),
                ),
            }
        )
        self._retry_after = self._retry_after or self.get_app_config(
            C.HEADER_RETRY_AFTER_VALUE
        )
        self._key_prefix = self._key_prefix or self.get_app_config(C.KEY_PREFIX)
        app_limits: Optional[StrOrCallableStr] = self.get_app_config(
            C.APPLICATION_LIMITS, None
        )
        if not self._application_limits and app_limits:
            self._application_limits = [
                LimitGroup(
                    app_limits,
                    self._key_func,
                    "global",
                    False,
                    None,
                    None,
                    None,
                    1,
                    False,
                )
            ]

        conf_limits: Optional[StrOrCallableStr] = self.get_app_config(
            C.DEFAULT_LIMITS, None
        )
        if not self._default_limits and conf_limits:
            self._default_limits = [
                LimitGroup(
                    conf_limits, self._key_func, None, False, None, None, None, 1, False
                )
            ]
        fallback_enabled = self.get_app_config(C.IN_MEMORY_FALLBACK_ENABLED, False)
        fallback_limits: Optional[StrOrCallableStr] = self.get_app_config(
            C.IN_MEMORY_FALLBACK, None
        )
        if not self._in_memory_fallback and fallback_limits:
            self._in_memory_fallback = [
                LimitGroup(
                    fallback_limits,
                    self._key_func,
                    None,
                    False,
                    None,
                    None,
                    None,
                    1,
                    False,
                )
            ]
        if not self._in_memory_fallback_enabled:
            self._in_memory_fallback_enabled = (
                fallback_enabled or len(self._in_memory_fallback) > 0
            )

        if self._in_memory_fallback_enabled:
            self._fallback_storage = MemoryStorage()
            self._fallback_limiter = STRATEGIES[strategy](self._fallback_storage)

    def slowapi_startup(self) -> None:
        """
        Starlette startup event handler that links the app with the Limiter instance.
        """
        pass

    def get_app_config(self, key: str, default_value: T = None) -> T:
        """
        Place holder until we find a better way to load config from app
        """
        pass

    def __should_check_backend(self) -> bool:
        pass

    def reset(self) -> None:
        """
        resets the storage if it supports being reset
        """
        pass

    @property
    def limiter(self) -> RateLimiter:
        """
        The backend that keeps track of consumption of endpoints vs limits
        """
        pass

    def _inject_headers(
        self, response: Response, current_limit: Tuple[RateLimitItem, List[str]]
    ) -> Response:
        pass

    def _inject_asgi_headers(
        self, headers: MutableHeaders, current_limit: Tuple[RateLimitItem, List[str]]
    ) -> MutableHeaders:
        """
        Injects 'X-RateLimit-Reset', 'X-RateLimit-Remaining', 'X-RateLimit-Limit'
        and 'Retry-After' headers into :headers parameter if needed.

        Basically the same as _inject_headers, but without access to the Response object.
        -> supports ASGI Middlewares.
        """
        pass

    def __evaluate_limits(
        self, request: Request, endpoint: str, limits: List[Limit]
    ) -> None:
        pass

    def _determine_retry_time(self, retry_header_value) -> int:
        pass

    def _check_request_limit(
        self,
        request: Request,
        endpoint_func: Optional[Callable[..., Any]],
        in_middleware: bool = True,
    ) -> None:
        """
        Determine if the request is within limits
        """
        pass

    def __limit_decorator(
        self,
        limit_value: StrOrCallableStr,
        key_func: Optional[Callable[..., str]] = None,
        shared: bool = False,
        scope: Optional[StrOrCallableStr] = None,
        per_method: bool = False,
        methods: Optional[List[str]] = None,
        error_message: Optional[str] = None,
        exempt_when: Optional[Callable[..., bool]] = None,
        cost: Union[int, Callable[..., int]] = 1,
        override_defaults: bool = True,
    ) -> Callable[..., Any]:
        pass

    def limit(
        self,
        limit_value: StrOrCallableStr,
        key_func: Optional[Callable[..., str]] = None,
        per_method: bool = False,
        methods: Optional[List[str]] = None,
        error_message: Optional[str] = None,
        exempt_when: Optional[Callable[..., bool]] = None,
        cost: Union[int, Callable[..., int]] = 1,
        override_defaults: bool = True,
    ) -> Callable:
        """
        Decorator to be used for rate limiting individual routes.

        * **limit_value**: rate limit string or a callable that returns a string.
         :ref:`ratelimit-string` for more details.
        * **key_func**: function/lambda to extract the unique identifier for
         the rate limit. defaults to remote address of the request.
        * **per_method**: whether the limit is sub categorized into the http
         method of the request.
        * **methods**: if specified, only the methods in this list will be rate
         limited (default: None).
        * **error_message**: string (or callable that returns one) to override the
         error message used in the response.
        * **exempt_when**: function returning a boolean indicating whether to exempt
        the route from the limit. This function can optionally use a Request object.
        * **cost**: integer (or callable that returns one) which is the cost of a hit
        * **override_defaults**: whether to override the default limits (default: True)
        """
        pass

    def shared_limit(
        self,
        limit_value: StrOrCallableStr,
        scope: StrOrCallableStr,
        key_func: Optional[Callable[..., str]] = None,
        error_message: Optional[str] = None,
        exempt_when: Optional[Callable[..., bool]] = None,
        cost: Union[int, Callable[..., int]] = 1,
        override_defaults: bool = True,
    ) -> Callable:
        """
        Decorator to be applied to multiple routes sharing the same rate limit.

        * **limit_value**: rate limit string or a callable that returns a string.
         :ref:`ratelimit-string` for more details.
        * **scope**: a string or callable that returns a string
         for defining the rate limiting scope.
        * **key_func**: function/lambda to extract the unique identifier for
         the rate limit. defaults to remote address of the request.
        * **per_method**: whether the limit is sub categorized into the http
         method of the request.
        * **methods**: if specified, only the methods in this list will be rate
         limited (default: None).
        * **error_message**: string (or callable that returns one) to override the
         error message used in the response.
        * **exempt_when**: function returning a boolean indicating whether to exempt
        the route from the limit
        * **cost**: integer (or callable that returns one) which is the cost of a hit
        * **override_defaults**: whether to override the default limits (default: True)
        """
        pass

    def exempt(self, obj):
        """
        Decorator to mark a view as exempt from rate limits.
        """
        pass
