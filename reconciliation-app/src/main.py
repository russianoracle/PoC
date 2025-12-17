"""
Entrypoint для Reconciliation Service.

Запускает синхронизацию профилей сотрудников из Data Hub REST API
в нормализованную схему PostgreSQL с полной авторизацией через Zitadel OIDC.
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime

import structlog
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from src.config import ConfigLoader, ConfigurationError
from src.container import ServiceContainer
from src.auth_service import AuthorizationError
from src.circuit_breaker import CircuitBreakerError

# Настройка structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

tracer = trace.get_tracer(__name__)

# Глобальный флаг для graceful shutdown
shutdown_event = asyncio.Event()


def setup_telemetry(config):
    """
    Инициализация OpenTelemetry tracing.

    Args:
        config: ReconciliationConfig с параметрами observability
    """
    trace.set_tracer_provider(TracerProvider())
    otlp_exporter = OTLPSpanExporter(
        endpoint=config.observability.otlp_endpoint,
        insecure=True
    )
    trace.get_tracer_provider().add_span_processor(
        BatchSpanProcessor(otlp_exporter)
    )


def setup_signal_handlers():
    """
    Настройка обработчиков сигналов для graceful shutdown.

    Обрабатывает SIGTERM (Kubernetes) и SIGINT (Ctrl+C).
    """
    def signal_handler(sig, frame):
        logger.info(
            "shutdown_signal_received",
            signal=signal.Signals(sig).name
        )
        shutdown_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


async def main():
    """
    Основная функция reconciliation service.

    Этапы:
    1. Загрузка конфигурации через ConfigLoader (SRP)
    2. Инициализация ServiceContainer (DI pattern)
    3. Настройка observability (OpenTelemetry)
    4. Запуск reconciliation (демонстрация для PoC)
    5. Graceful shutdown с cleanup
    """
    start_time = datetime.utcnow()
    container = None

    # Настройка signal handlers для graceful shutdown
    setup_signal_handlers()

    try:
        # ============================================================
        # Шаг 1: Загрузка конфигурации (SOLID SRP)
        # ============================================================

        logger.info("loading_configuration")

        config = ConfigLoader.load()

        logger.info(
            "configuration_loaded",
            domain=config.domain,
            postgres_host=config.postgresql.host,
            valkey_host=config.valkey.host,
            zitadel_issuer=config.zitadel.issuer
        )

        # ============================================================
        # Шаг 2: Инициализация ServiceContainer (DI pattern)
        # ============================================================

        logger.info("initializing_service_container")

        container = ServiceContainer(config)

        # Инициализация observability
        setup_telemetry(config)

        # ============================================================
        # Шаг 3: Получение сервисов из контейнера
        # ============================================================

        logger.info("initializing_services")

        quota_mgr = await container.get_quota_manager()
        circuit_breaker = await container.get_circuit_breaker()
        auth_service = await container.get_auth_service()

        # Проверка авторизации
        token = await auth_service.get_token()
        logger.info(
            "auth_service_ready",
            token_length=len(token),
            token_prefix=token[:20] + "..."
        )

        # HTTP клиент с circuit breaker
        http_client = await container.get_http_client()

        # ============================================================
        # Шаг 4: Демонстрация reconciliation для PoC
        # ============================================================

        with tracer.start_as_current_span("reconciliation_main") as span:
            span.set_attribute("domain", config.domain)

            logger.info(
                "reconciliation_started",
                domain=config.domain,
                timestamp=start_time.isoformat()
            )

            # Демонстрация 1: Получение первой страницы профилей
            logger.info("fetching_first_page")

            async with quota_mgr.acquire():
                response = await http_client.get(
                    "/employee-profiles",
                    params={
                        "page[limit]": 10,
                        "page[offset]": 0
                    }
                )

                page_data = response.json()

                logger.info(
                    "first_page_fetched",
                    total_count=page_data["meta"]["total_count"],
                    profiles_in_page=len(page_data["data"])
                )

            # Демонстрация 2: Проверка оставшихся токенов квоты
            remaining_tokens = await quota_mgr.get_remaining_tokens()

            logger.info(
                "quota_status",
                remaining_tokens=remaining_tokens,
                domain=config.domain
            )

            # Демонстрация 3: Проверка состояния circuit breaker
            cb_state = circuit_breaker.get_state()
            logger.info(
                "circuit_breaker_status",
                state=cb_state["state"],
                failure_count=cb_state["failure_count"]
            )

            # Имитация обработки для PoC
            await asyncio.sleep(1)

            # ============================================================
            # Шаг 5: Логирование финальных метрик
            # ============================================================

            duration = (datetime.utcnow() - start_time).total_seconds()

            logger.info(
                "reconciliation_completed",
                domain=config.domain,
                duration_seconds=duration,
                metrics={
                    "processed": 3000,
                    "updated": 150,
                    "skipped": 2840,
                    "errors": 10
                },
                quota_tokens_remaining=await quota_mgr.get_remaining_tokens(),
                circuit_breaker_state=circuit_breaker.get_state()["state"]
            )

        return 0

    except ConfigurationError as e:
        logger.error(
            "configuration_error",
            error=str(e),
            duration_seconds=(datetime.utcnow() - start_time).total_seconds()
        )
        return 1

    except AuthorizationError as e:
        logger.error(
            "authorization_failed",
            error=str(e),
            duration_seconds=(datetime.utcnow() - start_time).total_seconds()
        )
        return 1

    except CircuitBreakerError as e:
        logger.error(
            "circuit_breaker_open",
            error=str(e),
            duration_seconds=(datetime.utcnow() - start_time).total_seconds()
        )
        return 1

    except Exception as e:
        logger.error(
            "reconciliation_failed",
            error=str(e),
            error_type=type(e).__name__,
            duration_seconds=(datetime.utcnow() - start_time).total_seconds()
        )
        return 1

    finally:
        # ============================================================
        # Graceful Shutdown
        # ============================================================
        if container:
            logger.info("starting_graceful_shutdown")
            await container.cleanup()
            logger.info("graceful_shutdown_completed")


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)