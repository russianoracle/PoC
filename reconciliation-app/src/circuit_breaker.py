"""
Circuit Breaker pattern для защиты от cascading failures при вызовах Data Hub API.

Реализует Reactive Resilience pattern (часть Reactive Manifesto).
"""

import asyncio
import time
import logging
from enum import Enum
from typing import Callable, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    """Состояния Circuit Breaker."""
    CLOSED = "closed"  # Нормальная работа
    OPEN = "open"  # Цепь разомкнута (запросы блокируются)
    HALF_OPEN = "half_open"  # Тестирование восстановления


@dataclass
class CircuitBreakerConfig:
    """Конфигурация Circuit Breaker."""
    failure_threshold: int = 5  # Количество ошибок до открытия цепи
    timeout: float = 60.0  # Время в секундах до перехода в HALF_OPEN
    success_threshold: int = 2  # Успешных попыток для закрытия цепи


class CircuitBreakerError(Exception):
    """Исключение при открытой цепи."""
    pass


class CircuitBreaker:
    """
    Circuit Breaker для защиты от cascading failures.

    Состояния:
    - CLOSED: запросы проходят, отслеживаются ошибки
    - OPEN: запросы блокируются немедленно
    - HALF_OPEN: пропускается ограниченное количество запросов для тестирования

    Пример использования:

        circuit_breaker = CircuitBreaker("datahub_api")

        try:
            result = await circuit_breaker.call(http_client.get, "/endpoint")
        except CircuitBreakerError:
            # Fallback logic
            result = get_cached_data()
    """

    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        """
        Инициализация Circuit Breaker.

        Args:
            name: Имя circuit breaker (для логирования)
            config: Конфигурация (использует defaults если None)
        """
        self.name = name
        self.config = config or CircuitBreakerConfig()

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None

        self._lock = asyncio.Lock()

        logger.info(
            f"CircuitBreaker '{name}' initialized: "
            f"failure_threshold={self.config.failure_threshold}, "
            f"timeout={self.config.timeout}s, "
            f"success_threshold={self.config.success_threshold}"
        )

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Вызов функции через Circuit Breaker.

        Args:
            func: Async функция для вызова
            *args, **kwargs: Аргументы функции

        Returns:
            Результат вызова функции

        Raises:
            CircuitBreakerError: Если цепь открыта
            Exception: Исключение из вызываемой функции
        """
        async with self._lock:
            # Проверка состояния цепи
            if self.state == CircuitState.OPEN:
                await self._try_transition_to_half_open()

                if self.state == CircuitState.OPEN:
                    logger.warning(
                        f"CircuitBreaker '{self.name}' is OPEN, "
                        f"blocking request (failures={self.failure_count})"
                    )
                    raise CircuitBreakerError(
                        f"Circuit breaker '{self.name}' is open"
                    )

        # Выполнение запроса
        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result

        except Exception as e:
            await self._on_failure()
            raise

    async def _on_success(self):
        """Обработка успешного вызова."""
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1

                if self.success_count >= self.config.success_threshold:
                    self._transition_to_closed()
            else:
                # В CLOSED состоянии сбрасываем счетчик ошибок при успехе
                self.failure_count = 0

    async def _on_failure(self):
        """Обработка неудачного вызова."""
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == CircuitState.HALF_OPEN:
                # В HALF_OPEN любая ошибка → обратно в OPEN
                self._transition_to_open()

            elif self.failure_count >= self.config.failure_threshold:
                self._transition_to_open()

    async def _try_transition_to_half_open(self):
        """Попытка перехода из OPEN в HALF_OPEN после timeout."""
        if self.state != CircuitState.OPEN:
            return

        if self.last_failure_time is None:
            return

        elapsed = time.time() - self.last_failure_time

        if elapsed >= self.config.timeout:
            self._transition_to_half_open()

    def _transition_to_open(self):
        """Переход в состояние OPEN."""
        logger.error(
            f"CircuitBreaker '{self.name}' transition: {self.state.value} → OPEN "
            f"(failures={self.failure_count})"
        )

        self.state = CircuitState.OPEN
        self.success_count = 0

    def _transition_to_half_open(self):
        """Переход в состояние HALF_OPEN."""
        logger.info(
            f"CircuitBreaker '{self.name}' transition: {self.state.value} → HALF_OPEN "
            f"(testing recovery)"
        )

        self.state = CircuitState.HALF_OPEN
        self.success_count = 0

    def _transition_to_closed(self):
        """Переход в состояние CLOSED."""
        logger.info(
            f"CircuitBreaker '{self.name}' transition: {self.state.value} → CLOSED "
            f"(recovery successful, successes={self.success_count})"
        )

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None

    async def reset(self):
        """Принудительный сброс в CLOSED (для тестирования/восстановления)."""
        async with self._lock:
            self._transition_to_closed()

            logger.warning(f"CircuitBreaker '{self.name}' manually reset to CLOSED")

    def get_state(self) -> dict:
        """
        Получение текущего состояния Circuit Breaker.

        Returns:
            Словарь с метриками
        """
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": self.last_failure_time
        }
