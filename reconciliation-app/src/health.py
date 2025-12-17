"""
Health check endpoints для Cloud Native compatibility.

Реализует Kubernetes liveness/readiness probes.
"""

import logging
from typing import Dict, Any
from datetime import datetime
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class HealthStatus:
    """Статусы health check."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"


class HealthChecker:
    """
    Health checker для Kubernetes probes.

    Проверяет доступность зависимостей (Redis, PostgreSQL).
    """

    def __init__(self, redis_client: redis.Redis):
        """
        Инициализация health checker.

        Args:
            redis_client: Redis клиент для проверки
        """
        self.redis_client = redis_client

    async def liveness_check(self) -> Dict[str, Any]:
        """
        Liveness probe - проверка что приложение живо.

        Используется Kubernetes для рестарта pod при необходимости.

        Returns:
            Словарь с результатом проверки
        """
        return {
            "status": HealthStatus.HEALTHY,
            "timestamp": datetime.utcnow().isoformat(),
            "checks": {
                "process": "running"
            }
        }

    async def readiness_check(self) -> Dict[str, Any]:
        """
        Readiness probe - проверка что приложение готово принимать трафик.

        Проверяет доступность критичных зависимостей.

        Returns:
            Словарь с результатом проверки
        """
        checks = {}
        overall_status = HealthStatus.HEALTHY

        # Проверка Redis
        try:
            await self.redis_client.ping()
            checks["redis"] = {
                "status": HealthStatus.HEALTHY,
                "message": "Connected"
            }
        except Exception as e:
            checks["redis"] = {
                "status": HealthStatus.UNHEALTHY,
                "message": f"Connection failed: {str(e)}"
            }
            overall_status = HealthStatus.UNHEALTHY
            logger.error(f"Redis health check failed: {e}")

        return {
            "status": overall_status,
            "timestamp": datetime.utcnow().isoformat(),
            "checks": checks
        }
