"""
Unit of Work pattern для управления транзакциями.

Координирует работу репозиториев и обеспечивает атомарность операций.
"""

import logging
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.database import DatabaseSessionManager
from src.repository import ProfileRepository

logger = logging.getLogger(__name__)


class UnitOfWork:
    """
    Unit of Work для управления транзакциями и репозиториями.

    Обеспечивает атомарность операций с несколькими репозиториями.
    Работает как async context manager.

    Пример использования:

        async with UnitOfWork(db_manager) as uow:
            profile = await uow.profile_repository.get_by_prid("enp6ejAwMA")
            profile.checksum = new_checksum
            await uow.commit()
    """

    def __init__(self, db_manager: DatabaseSessionManager):
        """
        Инициализация Unit of Work.

        Args:
            db_manager: Database session manager
        """
        self.db_manager = db_manager
        self._session: Optional[AsyncSession] = None
        self._profile_repository: Optional[ProfileRepository] = None

    async def __aenter__(self) -> "UnitOfWork":
        """
        Начало Unit of Work (открытие транзакции).

        Returns:
            UnitOfWork instance
        """
        # Создание async session
        self._session = self.db_manager.session_factory()

        # Инициализация репозиториев
        self._profile_repository = ProfileRepository(self._session)

        logger.debug("UnitOfWork started")

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Завершение Unit of Work.

        Автоматический rollback при exception, иначе требуется явный commit.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Exception traceback
        """
        if exc_type is not None:
            # Exception произошел → rollback
            await self.rollback()
            logger.error(
                "uow_rollback_on_exception",
                exception_type=exc_type.__name__,
                exception=str(exc_val)
            )
        else:
            # Если commit не был вызван явно, делаем rollback
            if self._session.in_transaction():
                logger.warning(
                    "uow_auto_rollback",
                    message="Transaction not committed, rolling back"
                )
                await self.rollback()

        await self._session.close()
        self._session = None
        self._profile_repository = None

        logger.debug("UnitOfWork closed")

    @property
    def profile_repository(self) -> ProfileRepository:
        """
        Получение ProfileRepository.

        Returns:
            ProfileRepository instance

        Raises:
            RuntimeError: Если UnitOfWork не активен
        """
        if self._profile_repository is None:
            raise RuntimeError(
                "UnitOfWork is not active. Use 'async with UnitOfWork(...)' "
                "context manager."
            )

        return self._profile_repository

    async def commit(self):
        """
        Commit транзакции.

        Все изменения, сделанные через репозитории, будут применены к БД.

        Raises:
            RuntimeError: Если UnitOfWork не активен
        """
        if self._session is None:
            raise RuntimeError("UnitOfWork is not active")

        await self._session.commit()

        logger.debug("uow_committed")

    async def rollback(self):
        """
        Rollback транзакции.

        Все изменения, сделанные через репозитории, будут отменены.

        Raises:
            RuntimeError: Если UnitOfWork не активен
        """
        if self._session is None:
            raise RuntimeError("UnitOfWork is not active")

        await self._session.rollback()

        logger.debug("uow_rolled_back")

    async def flush(self):
        """
        Flush изменений в БД без commit.

        Полезно для получения auto-generated IDs перед commit.

        Raises:
            RuntimeError: Если UnitOfWork не активен
        """
        if self._session is None:
            raise RuntimeError("UnitOfWork is not active")

        await self._session.flush()

        logger.debug("uow_flushed")
