"""
Repository pattern для доступа к данным профилей.

Инкапсулирует логику работы с ORM моделями и БД операциями (SOLID SRP + DIP).
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from src.models import (
    EmployeeProfile,
    Person,
    Employee,
    Position,
    LegalEntity,
    Job,
    Department,
    EntityPridMapping
)

logger = logging.getLogger(__name__)


class ProfileRepository:
    """
    Repository для операций с профилями сотрудников.

    Инкапсулирует SQL операции и ORM mapping. Работает через SQLAlchemy async session.
    """

    def __init__(self, session: AsyncSession):
        """
        Инициализация ProfileRepository.

        Args:
            session: SQLAlchemy async session (управляется UnitOfWork)
        """
        self.session = session

    async def get_by_prid(self, prid: str) -> Optional[EmployeeProfile]:
        """
        Получение профиля по PRID.

        Args:
            prid: Profile ID (например, "enp6ejAwMA")

        Returns:
            EmployeeProfile или None если не найден
        """
        result = await self.session.execute(
            select(EmployeeProfile)
            .where(EmployeeProfile.prid == prid)
            .where(EmployeeProfile.deleted_at.is_(None))  # Исключаем soft-deleted
        )

        profile = result.scalar_one_or_none()

        if profile:
            logger.debug("profile_found", prid=prid)
        else:
            logger.debug("profile_not_found", prid=prid)

        return profile

    async def get_profiles_changed_since(
        self,
        since: datetime,
        limit: int = 100
    ) -> List[EmployeeProfile]:
        """
        Получение профилей, измененных после указанной даты.

        Используется для incremental reconciliation.

        Args:
            since: Дата/время последней синхронизации
            limit: Максимальное количество профилей

        Returns:
            Список EmployeeProfile
        """
        result = await self.session.execute(
            select(EmployeeProfile)
            .where(EmployeeProfile.source_updated_at > since)
            .where(EmployeeProfile.deleted_at.is_(None))
            .order_by(EmployeeProfile.source_updated_at)
            .limit(limit)
        )

        profiles = result.scalars().all()

        logger.debug(
            "profiles_changed_since_fetched",
            since=since.isoformat(),
            count=len(profiles)
        )

        return list(profiles)

    async def upsert_profile(
        self,
        prid: str,
        person_id: UUID,
        legal_entity_id: Optional[UUID],
        email: Optional[str],
        checksum: str,
        source_updated_at: datetime,
        **kwargs
    ) -> EmployeeProfile:
        """
        UPSERT профиля сотрудника (PostgreSQL on_conflict_do_update).

        Args:
            prid: Profile ID
            person_id: UUID персоны
            legal_entity_id: UUID юридического лица
            email: Email сотрудника
            checksum: SHA256 checksum для reconciliation
            source_updated_at: Timestamp последнего изменения в источнике
            **kwargs: Дополнительные поля (location, acc_type)

        Returns:
            EmployeeProfile instance (attached to session)
        """
        stmt = insert(EmployeeProfile).values(
            prid=prid,
            person_id=person_id,
            legal_entity_id=legal_entity_id,
            email=email,
            checksum=checksum,
            source_updated_at=source_updated_at,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            **kwargs
        )

        # PostgreSQL-specific on_conflict_do_update
        stmt = stmt.on_conflict_do_update(
            index_elements=['prid'],
            set_={
                'person_id': stmt.excluded.person_id,
                'legal_entity_id': stmt.excluded.legal_entity_id,
                'email': stmt.excluded.email,
                'checksum': stmt.excluded.checksum,
                'source_updated_at': stmt.excluded.source_updated_at,
                'updated_at': datetime.utcnow(),
                **{k: getattr(stmt.excluded, k) for k in kwargs.keys()}
            }
        )

        await self.session.execute(stmt)

        # Получаем upserted объект для возврата
        profile = await self.get_by_prid(prid)

        logger.info("profile_upserted", prid=prid, checksum=checksum)

        return profile

    async def update_checksum(self, prid: str, new_checksum: str):
        """
        Обновление checksum профиля.

        Используется для reconciliation - если checksum совпадает, профиль не изменился.

        Args:
            prid: Profile ID
            new_checksum: Новый SHA256 checksum
        """
        stmt = (
            update(EmployeeProfile)
            .where(EmployeeProfile.prid == prid)
            .values(
                checksum=new_checksum,
                updated_at=datetime.utcnow()
            )
        )

        await self.session.execute(stmt)

        logger.debug("profile_checksum_updated", prid=prid)

    async def soft_delete(self, prid: str):
        """
        Soft delete профиля (проставление deleted_at).

        Args:
            prid: Profile ID
        """
        stmt = (
            update(EmployeeProfile)
            .where(EmployeeProfile.prid == prid)
            .values(
                deleted_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
        )

        await self.session.execute(stmt)

        logger.info("profile_soft_deleted", prid=prid)

    async def get_by_person_uuid(self, person_uuid: UUID) -> Optional[EmployeeProfile]:
        """
        Получение профиля по UUID персоны.

        Args:
            person_uuid: UUID из ps_persons.id

        Returns:
            EmployeeProfile или None
        """
        result = await self.session.execute(
            select(EmployeeProfile)
            .where(EmployeeProfile.person_id == person_uuid)
            .where(EmployeeProfile.deleted_at.is_(None))
        )

        return result.scalar_one_or_none()

    async def bulk_upsert(
        self,
        profiles_data: List[Dict[str, Any]]
    ) -> int:
        """
        Batch UPSERT профилей.

        Используется для initial load или массовых обновлений.

        Args:
            profiles_data: Список словарей с данными профилей

        Returns:
            Количество upserted профилей
        """
        if not profiles_data:
            return 0

        stmt = insert(EmployeeProfile).values(profiles_data)

        stmt = stmt.on_conflict_do_update(
            index_elements=['prid'],
            set_={
                'person_id': stmt.excluded.person_id,
                'legal_entity_id': stmt.excluded.legal_entity_id,
                'email': stmt.excluded.email,
                'checksum': stmt.excluded.checksum,
                'source_updated_at': stmt.excluded.source_updated_at,
                'updated_at': datetime.utcnow()
            }
        )

        await self.session.execute(stmt)

        count = len(profiles_data)

        logger.info("bulk_upsert_completed", count=count)

        return count

    async def count_profiles(self) -> int:
        """
        Подсчет общего количества профилей (без soft-deleted).

        Returns:
            Количество профилей
        """
        result = await self.session.execute(
            select(EmployeeProfile)
            .where(EmployeeProfile.deleted_at.is_(None))
        )

        profiles = result.scalars().all()

        return len(profiles)
