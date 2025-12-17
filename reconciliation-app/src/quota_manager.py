"""
Универсальный механизм управления квотами для Data Hub API.

Используется во всех 4 доменах (Профиль, Доверенности, Подписи, Подписание документов)
для предотвращения перегрузки Data Hub.

Алгоритм: Token Bucket (ведро токенов)
- Каждый домен имеет свой bucket с лимитом requests/hour
- Токены добавляются с заданной скоростью (refill_rate)
- Перед каждым HTTP запросом забирается токен
- Если токенов нет → ожидание до пополнения
"""

import asyncio
import time
import logging
from typing import Optional
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class QuotaManager:
    """
    Универсальный менеджер квот для Data Hub API.

    Использует Token Bucket алгоритм через Redis для координации между
    множественными экземплярами reconciliation jobs (для разных доменов).

    Пример использования:

        quota_mgr = QuotaManager(redis_client, domain="profiles", max_requests_per_hour=50)

        async with quota_mgr.acquire():
            response = await http_client.get("/employee-profiles")
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        domain: str,
        max_requests_per_hour: int = 50,
        burst_size: int = 10
    ):
        """
        Инициализация QuotaManager.

        Args:
            redis_client: Async Redis клиент (ValKey compatible)
            domain: Название домена (profiles, authorities, signatures, document_signing)
            max_requests_per_hour: Максимальное количество запросов в час
            burst_size: Размер burst (пиковая нагрузка)
        """
        self.redis = redis_client
        self.domain = domain
        self.max_requests_per_hour = max_requests_per_hour
        self.burst_size = burst_size

        # Redis keys для token bucket
        self.bucket_key = f"quota:{domain}:bucket"
        self.timestamp_key = f"quota:{domain}:last_refill"

        # Refill rate (tokens per second)
        self.refill_rate = max_requests_per_hour / 3600.0

        logger.info(
            f"QuotaManager initialized for domain={domain}, "
            f"max_requests_per_hour={max_requests_per_hour}, "
            f"burst_size={burst_size}, "
            f"refill_rate={self.refill_rate:.4f} tokens/sec"
        )

    async def acquire(self, cost: int = 1, timeout: Optional[float] = None) -> bool:
        """
        Получение токена для HTTP запроса.

        Args:
            cost: Стоимость операции в токенах (обычно 1)
            timeout: Максимальное время ожидания в секундах (None = ждать бесконечно)

        Returns:
            True если токен получен

        Raises:
            TimeoutError: Если не удалось получить токен за timeout
        """
        start_time = time.time()
        retry_count = 0

        while True:
            # Попытка забрать токен
            success = await self._try_acquire(cost)

            if success:
                logger.debug(
                    f"Token acquired for domain={self.domain}, "
                    f"cost={cost}, retry_count={retry_count}"
                )
                return True

            # Проверка timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise TimeoutError(
                        f"Failed to acquire token for domain={self.domain} "
                        f"within {timeout}s (tried {retry_count} times)"
                    )

            # Ожидание перед retry
            retry_count += 1

            # Логирование каждые 10 попыток
            if retry_count % 10 == 0:
                logger.warning(
                    f"Waiting for token: domain={self.domain}, "
                    f"retry_count={retry_count}, elapsed={time.time() - start_time:.1f}s"
                )

            await asyncio.sleep(0.5)  # Retry каждые 500ms

    async def _try_acquire(self, cost: int) -> bool:
        """
        Попытка забрать токен (атомарная операция через Lua script).

        Returns:
            True если токен получен, False если bucket пуст
        """
        # Lua script для атомарной операции Token Bucket
        lua_script = """
        local bucket_key = KEYS[1]
        local timestamp_key = KEYS[2]
        local now = tonumber(ARGV[1])
        local cost = tonumber(ARGV[2])
        local max_tokens = tonumber(ARGV[3])
        local refill_rate = tonumber(ARGV[4])

        -- Получить текущее состояние bucket
        local tokens = tonumber(redis.call('GET', bucket_key) or max_tokens)
        local last_refill = tonumber(redis.call('GET', timestamp_key) or now)

        -- Refill tokens based on elapsed time
        local elapsed = now - last_refill
        local new_tokens = math.min(max_tokens, tokens + (elapsed * refill_rate))

        -- Проверка доступности токенов
        if new_tokens >= cost then
            -- Забрать токены
            redis.call('SET', bucket_key, new_tokens - cost)
            redis.call('SET', timestamp_key, now)

            -- Установить TTL для очистки неактивных buckets (24 часа)
            redis.call('EXPIRE', bucket_key, 86400)
            redis.call('EXPIRE', timestamp_key, 86400)

            return 1
        else
            return 0
        end
        """

        now = time.time()

        result = await self.redis.eval(
            lua_script,
            2,  # Number of keys
            self.bucket_key,
            self.timestamp_key,
            now,
            cost,
            self.burst_size,
            self.refill_rate
        )

        return result == 1

    async def get_remaining_tokens(self) -> float:
        """
        Получение количества оставшихся токенов в bucket.

        Returns:
            Количество доступных токенов (может быть дробным)
        """
        now = time.time()

        tokens = await self.redis.get(self.bucket_key)
        last_refill = await self.redis.get(self.timestamp_key)

        if tokens is None or last_refill is None:
            return float(self.burst_size)

        tokens = float(tokens)
        last_refill = float(last_refill)

        # Рассчитать refilled tokens
        elapsed = now - last_refill
        new_tokens = min(self.burst_size, tokens + (elapsed * self.refill_rate))

        return new_tokens

    async def reset(self):
        """
        Сброс bucket (для тестирования или восстановления после сбоя).
        """
        await self.redis.delete(self.bucket_key)
        await self.redis.delete(self.timestamp_key)

        logger.info(f"Token bucket reset for domain={self.domain}")

    def __aenter__(self):
        """Async context manager entry."""
        return self.acquire()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass


class DataHubQuotaConfig:
    """
    Конфигурация квот для всех доменов Data Hub (Фаза 1).

    Staggered schedule для предотвращения peak load:
    - Profiles: :00 минута → 40 requests/hour
    - Authorities: :15 минута → 40 requests/hour
    - Signatures: :30 минута → 40 requests/hour
    - Document Signing: :45 минута → 40 requests/hour

    Итого: ~160 requests/hour steady state, ~40 requests/burst каждые 15 минут
    """

    DOMAIN_CONFIGS = {
        "profiles": {
            "max_requests_per_hour": 50,  # Запас 25%
            "burst_size": 10,
            "cron_schedule": "0 * * * *"  # :00 минута
        },
        "authorities": {
            "max_requests_per_hour": 50,
            "burst_size": 10,
            "cron_schedule": "15 * * * *"  # :15 минута
        },
        "signatures": {
            "max_requests_per_hour": 50,
            "burst_size": 10,
            "cron_schedule": "30 * * * *"  # :30 минута
        },
        "document_signing": {
            "max_requests_per_hour": 50,
            "burst_size": 10,
            "cron_schedule": "45 * * * *"  # :45 минута
        }
    }

    @classmethod
    def get_config(cls, domain: str) -> dict:
        """
        Получение конфигурации квот для домена.

        Args:
            domain: Название домена

        Returns:
            Словарь с конфигурацией

        Raises:
            ValueError: Если домен не найден
        """
        if domain not in cls.DOMAIN_CONFIGS:
            raise ValueError(
                f"Unknown domain: {domain}. "
                f"Available domains: {list(cls.DOMAIN_CONFIGS.keys())}"
            )

        return cls.DOMAIN_CONFIGS[domain]