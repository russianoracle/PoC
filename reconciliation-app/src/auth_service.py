"""
Zitadel OIDC авторизация для межсервисных запросов к Data Hub API.

Реализует Client Credentials flow (Machine-to-Machine) с кешированием токенов в ValKey.
"""

import logging
from typing import Optional
from datetime import datetime, timedelta

import httpx
import redis.asyncio as redis
from opentelemetry import trace

from src.circuit_breaker import CircuitBreaker, CircuitBreakerError

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class AuthorizationError(Exception):
    """Ошибка авторизации."""
    pass


class ZitadelAuthService:
    """
    Сервис авторизации через Zitadel OIDC.

    Получает JWT токены для Service Account через Client Credentials flow
    и кеширует их в ValKey для переиспользования.

    Пример использования:

        auth_service = ZitadelAuthService(
            redis_client=redis_client,
            zitadel_issuer="https://zitadel.example.com",
            client_id="<CLIENT_ID>@reconciliation-service",
            client_secret="<CLIENT_SECRET>",
            scopes=["data_hub:people_service"]
        )

        # Получение токена (с кешированием)
        token = await auth_service.get_token()

        # Использование в HTTP запросе
        response = await http_client.get(
            "/employee-profiles",
            headers={"Authorization": f"Bearer {token}"}
        )
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        zitadel_issuer: str,
        client_id: str,
        client_secret: str,
        scopes: list[str]
    ):
        """
        Инициализация ZitadelAuthService.

        Args:
            redis_client: Async Redis клиент (ValKey compatible)
            zitadel_issuer: URL Zitadel instance (e.g., https://zitadel.example.com)
            client_id: Service Account Client ID
            client_secret: Service Account Client Secret
            scopes: Список scopes (e.g., ["data_hub:people_service"])
        """
        self.redis = redis_client
        self.issuer = zitadel_issuer.rstrip('/')
        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = scopes

        # Endpoints
        self.token_endpoint = f"{self.issuer}/oauth/v2/token"

        # Cache key
        self.cache_key = f"zitadel:token:{client_id}"

        logger.info(
            "ZitadelAuthService initialized",
            issuer=self.issuer,
            client_id=client_id,
            scopes=scopes
        )

    async def get_token(self) -> str:
        """
        Получение JWT токена (с кешированием).

        Workflow:
        1. Проверка кеша в ValKey
        2. Если кеш пуст или истек → запрос нового токена из Zitadel
        3. Кеширование нового токена с TTL (90% от expires_in)

        Returns:
            Valid JWT access_token

        Raises:
            AuthorizationError: Если не удалось получить токен
        """
        with tracer.start_as_current_span("get_token") as span:
            span.set_attribute("client_id", self.client_id)

            # Шаг 1: Проверка кеша
            cached_token = await self._get_cached_token()
            if cached_token:
                logger.debug(
                    "token_from_cache",
                    client_id=self.client_id,
                    cache_key=self.cache_key
                )
                span.set_attribute("cache_hit", True)
                return cached_token

            span.set_attribute("cache_hit", False)

            # Шаг 2: Получение нового токена
            logger.info(
                "fetching_new_token",
                client_id=self.client_id,
                token_endpoint=self.token_endpoint
            )

            token_response = await self._fetch_new_token()

            # Шаг 3: Кеширование
            await self._cache_token(
                token=token_response["access_token"],
                expires_in=token_response["expires_in"]
            )

            logger.info(
                "token_fetched_and_cached",
                client_id=self.client_id,
                expires_in=token_response["expires_in"],
                token_type=token_response["token_type"]
            )

            return token_response["access_token"]

    async def _get_cached_token(self) -> Optional[str]:
        """
        Получение токена из кеша ValKey.

        Returns:
            Cached token или None если кеш пуст
        """
        try:
            cached = await self.redis.get(self.cache_key)
            if cached:
                return cached.decode('utf-8')
        except Exception as e:
            logger.warning(
                "cache_read_error",
                error=str(e),
                cache_key=self.cache_key
            )

        return None

    async def _fetch_new_token(self) -> dict:
        """
        Получение нового JWT токена из Zitadel через Client Credentials flow.

        Returns:
            dict с ключами:
                - access_token: JWT токен
                - token_type: "Bearer"
                - expires_in: Срок действия в секундах

        Raises:
            AuthorizationError: Если запрос токена не удался
        """
        with tracer.start_as_current_span("fetch_new_token") as span:
            span.set_attribute("token_endpoint", self.token_endpoint)

            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        self.token_endpoint,
                        data={
                            "grant_type": "client_credentials",
                            "client_id": self.client_id,
                            "client_secret": self.client_secret,
                            "scope": " ".join(self.scopes)
                        },
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        timeout=10.0
                    )

                    if response.status_code != 200:
                        logger.error(
                            "token_fetch_failed",
                            status_code=response.status_code,
                            response_body=response.text,
                            client_id=self.client_id
                        )

                        span.set_attribute("error", True)
                        span.set_attribute("status_code", response.status_code)

                        raise AuthorizationError(
                            f"Failed to fetch token: HTTP {response.status_code}, "
                            f"response: {response.text}"
                        )

                    token_data = response.json()

                    # Валидация ответа
                    if "access_token" not in token_data:
                        raise AuthorizationError(
                            f"Invalid token response: missing access_token, "
                            f"response: {token_data}"
                        )

                    span.set_attribute("expires_in", token_data.get("expires_in", 0))

                    return token_data

            except httpx.RequestError as e:
                logger.error(
                    "token_fetch_network_error",
                    error=str(e),
                    token_endpoint=self.token_endpoint
                )
                span.record_exception(e)
                raise AuthorizationError(f"Network error fetching token: {e}")

    async def _cache_token(self, token: str, expires_in: int):
        """
        Кеширование токена в ValKey.

        Args:
            token: JWT access_token
            expires_in: Срок действия токена в секундах (из ответа Zitadel)
        """
        try:
            # Safety margin: кешируем на 90% от expires_in
            # Пример: если expires_in = 3600 сек (1 час), кешируем на 3240 сек (54 минуты)
            ttl = int(expires_in * 0.9)

            await self.redis.setex(
                self.cache_key,
                ttl,
                token
            )

            logger.debug(
                "token_cached",
                cache_key=self.cache_key,
                ttl=ttl,
                expires_at=(datetime.utcnow() + timedelta(seconds=ttl)).isoformat()
            )

        except Exception as e:
            logger.warning(
                "cache_write_error",
                error=str(e),
                cache_key=self.cache_key
            )
            # НЕ бросаем исключение - токен получен, просто не закеширован

    async def invalidate_cache(self):
        """
        Принудительная инвалидация кеша токена.

        Используется при получении HTTP 401 Unauthorized для принудительного обновления токена.
        """
        try:
            deleted_count = await self.redis.delete(self.cache_key)

            logger.info(
                "token_cache_invalidated",
                cache_key=self.cache_key,
                was_cached=deleted_count > 0
            )

        except Exception as e:
            logger.warning(
                "cache_invalidation_error",
                error=str(e),
                cache_key=self.cache_key
            )


class AuthenticatedHTTPClient:
    """
    HTTP клиент с автоматической авторизацией через Zitadel.

    Автоматически добавляет JWT токен в заголовок Authorization для всех запросов.
    Обрабатывает ошибки 401/403 с retry logic.

    Пример использования:

        auth_service = ZitadelAuthService(...)
        http_client = AuthenticatedHTTPClient(
            base_url="https://datahub.example.com",
            auth_service=auth_service
        )

        # Запрос с автоматической авторизацией
        response = await http_client.get("/employee-profiles", params={...})
    """

    def __init__(
        self,
        base_url: str,
        auth_service: ZitadelAuthService,
        timeout: float = 30.0,
        max_retries: int = 3,
        circuit_breaker: Optional[CircuitBreaker] = None
    ):
        """
        Инициализация AuthenticatedHTTPClient.

        Args:
            base_url: Базовый URL Data Hub API
            auth_service: Экземпляр ZitadelAuthService
            timeout: Timeout для HTTP запросов в секундах
            max_retries: Максимальное количество retry при ошибках авторизации
            circuit_breaker: Circuit breaker для защиты от cascading failures
        """
        self.base_url = base_url.rstrip('/')
        self.auth_service = auth_service
        self.timeout = timeout
        self.max_retries = max_retries
        self.circuit_breaker = circuit_breaker

        logger.info(
            "AuthenticatedHTTPClient initialized",
            base_url=self.base_url,
            timeout=timeout,
            max_retries=max_retries,
            circuit_breaker_enabled=circuit_breaker is not None
        )

    async def get(self, path: str, params: Optional[dict] = None, **kwargs) -> httpx.Response:
        """
        GET запрос с авторизацией.

        Args:
            path: Путь API (e.g., "/employee-profiles")
            params: Query parameters
            **kwargs: Дополнительные аргументы для httpx.get

        Returns:
            httpx.Response

        Raises:
            AuthorizationError: Если авторизация не удалась после всех retry
            httpx.HTTPStatusError: Для других HTTP ошибок
        """
        return await self._request("GET", path, params=params, **kwargs)

    async def post(self, path: str, json: Optional[dict] = None, **kwargs) -> httpx.Response:
        """POST запрос с авторизацией."""
        return await self._request("POST", path, json=json, **kwargs)

    async def patch(self, path: str, json: Optional[dict] = None, **kwargs) -> httpx.Response:
        """PATCH запрос с авторизацией."""
        return await self._request("PATCH", path, json=json, **kwargs)

    async def _request(
        self,
        method: str,
        path: str,
        retry_count: int = 0,
        **kwargs
    ) -> httpx.Response:
        """
        Внутренний метод для HTTP запросов с retry logic.

        Workflow:
        1. Получение JWT токена через auth_service
        2. Выполнение HTTP запроса с токеном в заголовке Authorization
        3. Обработка ошибок 401/403:
           - 401 → инвалидация кеша, retry с новым токеном
           - 403 → критическая ошибка (недостаточно прав)

        Args:
            method: HTTP метод (GET, POST, PATCH)
            path: Путь API
            retry_count: Счетчик попыток (internal)
            **kwargs: Аргументы для httpx.request

        Returns:
            httpx.Response

        Raises:
            AuthorizationError: Если авторизация не удалась после max_retries
        """
        with tracer.start_as_current_span(f"http_{method.lower()}") as span:
            span.set_attribute("method", method)
            span.set_attribute("path", path)
            span.set_attribute("retry_count", retry_count)

            # Шаг 1: Получение токена
            token = await self.auth_service.get_token()

            # Шаг 2: Подготовка заголовков
            headers = kwargs.pop("headers", {})
            headers["Authorization"] = f"Bearer {token}"
            headers.setdefault("Accept", "application/vnd.api+json")

            # Шаг 3: Выполнение запроса
            url = f"{self.base_url}{path}"

            # Определяем функцию запроса для оборачивания в circuit breaker
            async def make_request():
                async with httpx.AsyncClient() as client:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=headers,
                        timeout=self.timeout,
                        **kwargs
                    )

                    span.set_attribute("status_code", response.status_code)

                    # Обработка ошибок авторизации
                    if response.status_code == 401:
                        return await self._handle_401_error(
                            method, path, retry_count, response, **kwargs
                        )

                    if response.status_code == 403:
                        return await self._handle_403_error(response)

                    # Проверка других HTTP ошибок
                    response.raise_for_status()

                    logger.debug(
                        "http_request_success",
                        method=method,
                        path=path,
                        status_code=response.status_code
                    )

                    return response

            # Выполняем через circuit breaker если он настроен
            try:
                if self.circuit_breaker:
                    return await self.circuit_breaker.call(make_request)
                else:
                    return await make_request()

            except CircuitBreakerError as e:
                span.set_attribute("circuit_breaker_open", True)
                logger.error(
                    "circuit_breaker_open",
                    method=method,
                    path=path,
                    error=str(e)
                )
                raise

            except httpx.HTTPStatusError as e:
                span.record_exception(e)
                logger.error(
                    "http_request_error",
                    method=method,
                    path=path,
                    status_code=e.response.status_code,
                    response_body=e.response.text
                )
                raise

            except httpx.RequestError as e:
                span.record_exception(e)
                logger.error(
                    "http_request_network_error",
                    method=method,
                    path=path,
                    error=str(e)
                )
                raise

    async def _handle_401_error(
        self,
        method: str,
        path: str,
        retry_count: int,
        response: httpx.Response,
        **kwargs
    ) -> httpx.Response:
        """
        Обработка HTTP 401 Unauthorized.

        Причины 401:
        - Истек срок действия токена
        - Некорректные credentials
        - Service Account отключен в Zitadel

        Стратегия:
        1. Инвалидация кеша токена
        2. Retry с новым токеном (до max_retries)

        Raises:
            AuthorizationError: Если превышен max_retries
        """
        logger.warning(
            "http_401_unauthorized",
            method=method,
            path=path,
            retry_count=retry_count,
            response_body=response.text
        )

        if retry_count >= self.max_retries:
            raise AuthorizationError(
                f"Authorization failed after {self.max_retries} retries: "
                f"HTTP 401, response: {response.text}"
            )

        # Инвалидация кеша токена
        await self.auth_service.invalidate_cache()

        # Retry с новым токеном
        logger.info(
            "retrying_with_new_token",
            method=method,
            path=path,
            retry_count=retry_count + 1
        )

        return await self._request(
            method, path, retry_count=retry_count + 1, **kwargs
        )

    async def _handle_403_error(self, response: httpx.Response):
        """
        Обработка HTTP 403 Forbidden.

        Причины 403:
        - Недостаточно прав (scope отсутствует)
        - Service Account не имеет роли на проекте Data Hub

        Стратегия:
        Критическая ошибка конфигурации → FAIL FAST (не retry)

        Raises:
            AuthorizationError: Всегда
        """
        logger.error(
            "http_403_forbidden",
            required_scopes=self.auth_service.scopes,
            response_body=response.text,
            client_id=self.auth_service.client_id
        )

        raise AuthorizationError(
            f"Access denied (HTTP 403): Service Account lacks required scopes. "
            f"Required scopes: {self.auth_service.scopes}, "
            f"response: {response.text}"
        )