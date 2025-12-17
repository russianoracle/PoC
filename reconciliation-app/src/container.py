"""
Dependency Injection Container для Reconciliation Service.

Централизует создание и управление зависимостями (DI pattern, SOLID).
"""

import logging
import redis.asyncio as redis
from typing import Optional

from src.config import ReconciliationConfig
from src.auth_service import ZitadelAuthService, AuthenticatedHTTPClient
from src.quota_manager import QuotaManager, DataHubQuotaConfig
from src.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from src.database import DatabaseSessionManager
from src.unit_of_work import UnitOfWork

logger = logging.getLogger(__name__)


class ServiceContainer:
    """
    IoC Container для управления зависимостями сервисов.

    Создает и хранит singleton экземпляры сервисов с правильным lifecycle.
    """

    def __init__(self, config: ReconciliationConfig):
        """
        Инициализация контейнера.

        Args:
            config: Загруженная конфигурация
        """
        self.config = config

        # Service instances (lazy initialization)
        self._redis_client: Optional[redis.Redis] = None
        self._auth_service: Optional[ZitadelAuthService] = None
        self._quota_manager: Optional[QuotaManager] = None
        self._http_client: Optional[AuthenticatedHTTPClient] = None
        self._circuit_breaker: Optional[CircuitBreaker] = None
        self._db_manager: Optional[DatabaseSessionManager] = None

        logger.info(f"ServiceContainer initialized for domain={config.domain}")

    async def get_redis_client(self) -> redis.Redis:
        """
        Получение Redis (ValKey) клиента.

        Returns:
            Async Redis клиент
        """
        if self._redis_client is None:
            redis_url = (
                f"rediss://{self.config.valkey.host}:{self.config.valkey.port}"
                if self.config.valkey.ssl
                else f"redis://{self.config.valkey.host}:{self.config.valkey.port}"
            )

            self._redis_client = await redis.from_url(
                redis_url,
                password=self.config.valkey.password,
                decode_responses=False
            )

            # Проверка подключения
            await self._redis_client.ping()
            logger.info("Redis client connected")

        return self._redis_client

    async def get_circuit_breaker(self) -> CircuitBreaker:
        """
        Получение Circuit Breaker для Data Hub API.

        Returns:
            CircuitBreaker instance
        """
        if self._circuit_breaker is None:
            cb_config = CircuitBreakerConfig(
                failure_threshold=5,
                timeout=60.0,
                success_threshold=2
            )

            self._circuit_breaker = CircuitBreaker(
                name="datahub_api",
                config=cb_config
            )

        return self._circuit_breaker

    async def get_auth_service(self) -> ZitadelAuthService:
        """
        Получение Zitadel Auth Service.

        Returns:
            ZitadelAuthService instance
        """
        if self._auth_service is None:
            redis_client = await self.get_redis_client()

            self._auth_service = ZitadelAuthService(
                redis_client=redis_client,
                zitadel_issuer=self.config.zitadel.issuer,
                client_id=self.config.zitadel.client_id,
                client_secret=self.config.zitadel.client_secret,
                scopes=self.config.zitadel.scopes
            )

        return self._auth_service

    async def get_quota_manager(self) -> QuotaManager:
        """
        Получение Quota Manager.

        Returns:
            QuotaManager instance
        """
        if self._quota_manager is None:
            redis_client = await self.get_redis_client()
            quota_config = DataHubQuotaConfig.get_config(self.config.domain)

            self._quota_manager = QuotaManager(
                redis_client=redis_client,
                domain=self.config.domain,
                max_requests_per_hour=quota_config["max_requests_per_hour"],
                burst_size=quota_config["burst_size"]
            )

        return self._quota_manager

    async def get_http_client(self) -> AuthenticatedHTTPClient:
        """
        Получение HTTP клиента с авторизацией и circuit breaker.

        Returns:
            AuthenticatedHTTPClient instance
        """
        if self._http_client is None:
            auth_service = await self.get_auth_service()
            circuit_breaker = await self.get_circuit_breaker()

            self._http_client = AuthenticatedHTTPClient(
                base_url=self.config.datahub.api_url,
                auth_service=auth_service,
                timeout=float(self.config.datahub.timeout),
                circuit_breaker=circuit_breaker
            )

        return self._http_client

    async def get_database_manager(self) -> DatabaseSessionManager:
        """
        Получение Database Session Manager.

        Returns:
            DatabaseSessionManager instance
        """
        if self._db_manager is None:
            self._db_manager = DatabaseSessionManager(self.config.postgresql)

            logger.info("DatabaseSessionManager initialized")

        return self._db_manager

    def create_unit_of_work(self) -> UnitOfWork:
        """
        Создание нового Unit of Work.

        ВАЖНО: Каждый вызов создает НОВЫЙ UnitOfWork с собственной транзакцией.
        Используется через async context manager.

        Returns:
            UnitOfWork instance

        Example:
            async with container.create_unit_of_work() as uow:
                profile = await uow.profile_repository.get_by_prid("enp6ejAwMA")
                await uow.commit()
        """
        if self._db_manager is None:
            raise RuntimeError(
                "DatabaseSessionManager not initialized. "
                "Call get_database_manager() first."
            )

        return UnitOfWork(self._db_manager)

    async def cleanup(self):
        """
        Очистка ресурсов (graceful shutdown).

        Закрывает соединения с внешними системами.
        """
        logger.info("Cleaning up service container resources...")

        if self._redis_client is not None:
            try:
                await self._redis_client.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")

        if self._db_manager is not None:
            try:
                await self._db_manager.close()
                logger.info("Database engine closed")
            except Exception as e:
                logger.error(f"Error closing database engine: {e}")

        logger.info("Service container cleanup completed")
