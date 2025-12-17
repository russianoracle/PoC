"""
Configuration loader для Reconciliation Service.

Загружает конфигурацию из environment variables (Cloud Native 12-Factor).
Централизует конфигурацию (SRP).
"""

import os
import logging
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class PostgreSQLConfig:
    """Конфигурация PostgreSQL."""
    host: str
    port: int
    database: str
    user: str
    password: str
    ssl_mode: str


@dataclass
class ValKeyConfig:
    """Конфигурация ValKey (Redis)."""
    host: str
    port: int
    password: str
    ssl: bool


@dataclass
class ZitadelConfig:
    """Конфигурация Zitadel OIDC."""
    issuer: str
    client_id: str
    client_secret: str
    scopes: list[str]


@dataclass
class DataHubConfig:
    """Конфигурация Data Hub API."""
    api_url: str
    timeout: int


@dataclass
class ObservabilityConfig:
    """Конфигурация observability (OpenTelemetry)."""
    otlp_endpoint: str
    service_name: str
    resource_attributes: str


@dataclass
class ReconciliationConfig:
    """Общая конфигурация Reconciliation Service."""
    domain: str
    page_size: int
    timeout: int
    log_level: str
    log_format: str

    postgresql: PostgreSQLConfig
    valkey: ValKeyConfig
    zitadel: ZitadelConfig
    datahub: DataHubConfig
    observability: ObservabilityConfig


class ConfigurationError(Exception):
    """Ошибка загрузки конфигурации."""
    pass


class ConfigLoader:
    """
    Загрузчик конфигурации из environment variables.

    Валидирует обязательные параметры и возвращает typed configuration.
    """

    @staticmethod
    def load() -> ReconciliationConfig:
        """
        Загрузка конфигурации из environment variables.

        Returns:
            ReconciliationConfig с загруженными параметрами

        Raises:
            ConfigurationError: Если отсутствуют обязательные параметры
        """
        try:
            # PostgreSQL
            postgresql = PostgreSQLConfig(
                host=ConfigLoader._get_required("POSTGRES_HOST"),
                port=int(ConfigLoader._get_optional("POSTGRES_PORT", "5432")),
                database=ConfigLoader._get_optional("POSTGRES_DB", "eflow_profiles"),
                user=ConfigLoader._get_required("POSTGRES_USER"),
                password=ConfigLoader._get_required("POSTGRES_PASSWORD"),
                ssl_mode=ConfigLoader._get_optional("POSTGRES_SSL_MODE", "require")
            )

            # ValKey
            valkey = ValKeyConfig(
                host=ConfigLoader._get_required("VALKEY_HOST"),
                port=int(ConfigLoader._get_optional("VALKEY_PORT", "6379")),
                password=ConfigLoader._get_required("VALKEY_PASSWORD"),
                ssl=ConfigLoader._get_optional("VALKEY_SSL", "true").lower() == "true"
            )

            # Zitadel
            zitadel = ZitadelConfig(
                issuer=ConfigLoader._get_required("ZITADEL_ISSUER"),
                client_id=ConfigLoader._get_required("ZITADEL_CLIENT_ID"),
                client_secret=ConfigLoader._get_required("ZITADEL_CLIENT_SECRET"),
                scopes=ConfigLoader._get_optional(
                    "ZITADEL_SCOPES",
                    "data_hub:people_service"
                ).split()
            )

            # Data Hub
            datahub = DataHubConfig(
                api_url=ConfigLoader._get_required("DATA_HUB_API_URL"),
                timeout=int(ConfigLoader._get_optional("DATA_HUB_API_TIMEOUT", "30"))
            )

            # Observability
            observability = ObservabilityConfig(
                otlp_endpoint=ConfigLoader._get_optional(
                    "OTEL_EXPORTER_OTLP_ENDPOINT",
                    "http://otel-collector.observability.svc.cluster.local:4317"
                ),
                service_name=ConfigLoader._get_optional(
                    "OTEL_SERVICE_NAME",
                    "profile-reconciliation"
                ),
                resource_attributes=ConfigLoader._get_optional(
                    "OTEL_RESOURCE_ATTRIBUTES",
                    "service.version=v1.3.1,deployment.environment=production"
                )
            )

            # Общие параметры
            config = ReconciliationConfig(
                domain=ConfigLoader._get_optional("DOMAIN", "profiles"),
                page_size=int(ConfigLoader._get_optional("PAGE_SIZE", "100")),
                timeout=int(ConfigLoader._get_optional("RECONCILIATION_TIMEOUT", "600")),
                log_level=ConfigLoader._get_optional("LOG_LEVEL", "INFO"),
                log_format=ConfigLoader._get_optional("LOG_FORMAT", "json"),
                postgresql=postgresql,
                valkey=valkey,
                zitadel=zitadel,
                datahub=datahub,
                observability=observability
            )

            logger.info(
                f"Configuration loaded successfully: "
                f"domain={config.domain}, "
                f"postgres_host={config.postgresql.host}, "
                f"valkey_host={config.valkey.host}, "
                f"zitadel_issuer={config.zitadel.issuer}"
            )

            return config

        except ConfigurationError as e:
            logger.error(f"Configuration error: {e}")
            raise

        except Exception as e:
            logger.error(f"Unexpected error loading configuration: {e}")
            raise ConfigurationError(f"Failed to load configuration: {e}")

    @staticmethod
    def _get_required(key: str) -> str:
        """
        Получение обязательного параметра из environment variables.

        Args:
            key: Имя переменной окружения

        Returns:
            Значение переменной

        Raises:
            ConfigurationError: Если переменная не установлена
        """
        value = os.getenv(key)

        if value is None or value.strip() == "":
            raise ConfigurationError(
                f"Missing required environment variable: {key}"
            )

        return value

    @staticmethod
    def _get_optional(key: str, default: str) -> str:
        """
        Получение опционального параметра из environment variables.

        Args:
            key: Имя переменной окружения
            default: Значение по умолчанию

        Returns:
            Значение переменной или default
        """
        return os.getenv(key, default)
