"""
Database session management для Reconciliation Service.

Управляет async connection pool и session factory (SQLAlchemy 2.0).
"""

import logging
from typing import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine
)
from sqlalchemy.pool import NullPool

from src.config import PostgreSQLConfig

logger = logging.getLogger(__name__)


class DatabaseSessionManager:
    """
    Управление async database sessions.

    Создает connection pool и session factory для SQLAlchemy 2.0 async.
    """

    def __init__(self, config: PostgreSQLConfig):
        """
        Инициализация database session manager.

        Args:
            config: PostgreSQL конфигурация
        """
        self.config = config

        # Создание async engine
        database_url = self._build_database_url()

        self.engine: AsyncEngine = create_async_engine(
            database_url,
            echo=False,  # Set to True for SQL query logging
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,  # Проверка соединений перед использованием
            poolclass=NullPool if config.ssl_mode == "disable" else None
        )

        # Session factory
        self.session_factory = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
            autocommit=False
        )

        logger.info(
            "DatabaseSessionManager initialized",
            host=config.host,
            database=config.database,
            ssl_mode=config.ssl_mode
        )

    def _build_database_url(self) -> str:
        """
        Построение asyncpg database URL.

        Returns:
            Database URL для asyncpg
        """
        # asyncpg driver для async SQLAlchemy
        return (
            f"postgresql+asyncpg://{self.config.user}:{self.config.password}"
            f"@{self.config.host}:{self.config.port}/{self.config.database}"
            f"?ssl={self.config.ssl_mode}"
        )

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Context manager для async database session.

        Yields:
            AsyncSession instance

        Example:
            async with db_manager.session() as session:
                result = await session.execute(select(EmployeeProfile))
        """
        async with self.session_factory() as session:
            try:
                yield session
            except Exception as e:
                logger.error(
                    "database_session_error",
                    error=str(e),
                    error_type=type(e).__name__
                )
                await session.rollback()
                raise
            finally:
                await session.close()

    async def close(self):
        """
        Закрытие database engine (graceful shutdown).

        Вызывается при shutdown приложения для корректного закрытия всех соединений.
        """
        logger.info("Closing database engine")
        await self.engine.dispose()
        logger.info("Database engine closed")
