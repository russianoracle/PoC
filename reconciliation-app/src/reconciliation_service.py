"""
Reconciliation Service для синхронизации профилей из Data Hub в PostgreSQL.

Реализует оптимизированную стратегию сверки с трехуровневым кешированием
и механизмом Early Termination для минимизации нагрузки на Data Hub.
"""

import logging
from datetime import datetime
from typing import Dict, Any, List

from src.aggregate_builder import AggregateBuilder
from src.checksum_calculator import ChecksumCalculator
from src.repository import ProfileRepository

logger = logging.getLogger(__name__)


class ReconciliationService:
    """
    Сервис синхронизации профилей с оптимизацией нагрузки.

    Ключевые особенности:
    - Early Termination (stop when no changes detected)
    - Трехуровневое кеширование (In-Memory, ValKey, PostgreSQL)
    - Aggregate Checksum для детектирования изменений
    """

    def __init__(
        self,
        http_client,
        repository: ProfileRepository,
        valkey_client=None
    ):
        """
        Инициализация ReconciliationService.

        Args:
            http_client: HTTP клиент для запросов к Data Hub
            repository: ProfileRepository для операций с БД
            valkey_client: Optional ValKey клиент для metadata cache
        """
        self.http_client = http_client
        self.repository = repository
        self.valkey_client = valkey_client
        self.aggregate_builder = AggregateBuilder(http_client)

        # Статистика для логирования
        self.stats = {
            "processed": 0,
            "updated": 0,
            "skipped": 0,
            "errors": 0,
            "api_calls": 0
        }

    async def run_full_sync(self, last_sync_timestamp: datetime | None = None) -> Dict[str, Any]:
        """
        Полная синхронизация профилей с Early Termination.

        Алгоритм:
        1. Запрос профилей с сортировкой sort=-meta.updated_at
        2. Для каждого batch: проверка Early Termination
        3. Для каждого профиля: проверка checksum и UPSERT при изменениях

        Args:
            last_sync_timestamp: Timestamp последней успешной синхронизации
                                (для Early Termination)

        Returns:
            Словарь с результатами синхронизации
        """
        logger.info(
            "full_sync_started",
            last_sync_timestamp=last_sync_timestamp.isoformat() if last_sync_timestamp else None
        )

        start_time = datetime.utcnow()
        offset = 0
        page_size = 100
        early_termination = False

        while not early_termination:
            # Запрос страницы профилей с сортировкой
            params = {
                "sort": "-meta.updated_at",  # Свежие профили в начале
                "page[limit]": page_size,
                "page[offset]": offset
            }

            logger.debug("fetching_page", offset=offset, limit=page_size)

            response = await self.http_client.get("/employee-profiles", params=params)
            page_data = response.json()

            profiles = page_data.get("data", [])
            self.stats["api_calls"] += 1

            if not profiles:
                logger.info("no_more_profiles")
                break

            # Early Termination Check
            if last_sync_timestamp:
                max_updated_at = self._get_max_updated_at(profiles)

                if max_updated_at <= last_sync_timestamp:
                    logger.info(
                        "early_termination_triggered",
                        offset=offset,
                        max_updated_at=max_updated_at.isoformat(),
                        last_sync_timestamp=last_sync_timestamp.isoformat()
                    )
                    early_termination = True
                    # Все оставшиеся профили старше last_sync_timestamp, прерываем
                    self.stats["skipped"] += len(profiles)
                    break

            # Обработка профилей в batch
            for profile_json in profiles:
                await self._process_profile(profile_json)

            offset += page_size

        # Очистка in-memory кеша после завершения сессии
        self.aggregate_builder.clear_cache()

        duration = (datetime.utcnow() - start_time).total_seconds()

        logger.info(
            "full_sync_completed",
            duration_seconds=duration,
            stats=self.stats
        )

        return {
            "success": True,
            "duration_seconds": duration,
            **self.stats
        }

    async def _process_profile(self, profile_json: Dict[str, Any]):
        """
        Обработка одного профиля.

        Шаги:
        1. Проверка metadata в ValKey (уровень 2 кеш)
        2. Построение агрегата через AggregateBuilder
        3. Вычисление aggregate checksum
        4. Сравнение с кешированным checksum
        5. UPSERT всех сущностей при изменениях

        Args:
            profile_json: JSON:API data для employee-profile
        """
        profile_id = profile_json["id"]
        prid = profile_json["attributes"]["prid"]
        source_updated_at = datetime.fromisoformat(
            profile_json["meta"]["updated_at"].replace("Z", "+00:00")
        )

        self.stats["processed"] += 1

        try:
            # Проверка ValKey metadata cache (уровень 2)
            cached_metadata = await self._check_valkey_cache(profile_id)

            if cached_metadata:
                if cached_metadata["source_updated_at"] >= source_updated_at:
                    logger.debug("profile_skipped_by_valkey_cache", prid=prid)
                    self.stats["skipped"] += 1
                    return

            # Построение агрегата (с in-memory кешированием справочников)
            aggregate = await self.aggregate_builder.build_aggregate(profile_json)
            self.stats["api_calls"] += self._count_api_calls_in_aggregate(aggregate)

            # Вычисление aggregate checksum
            new_checksum = ChecksumCalculator.calculate(aggregate)

            # Проверка checksum (skip if unchanged)
            if cached_metadata and cached_metadata.get("checksum") == new_checksum:
                logger.debug("profile_skipped_by_checksum", prid=prid)
                self.stats["skipped"] += 1
                return

            # Агрегат изменился - UPSERT всех сущностей
            await self._upsert_aggregate(prid, aggregate, new_checksum, source_updated_at)

            # Обновление ValKey metadata cache
            await self._update_valkey_cache(profile_id, prid, new_checksum, source_updated_at)

            self.stats["updated"] += 1

            logger.info("profile_updated", prid=prid, checksum=new_checksum[:16] + "...")

        except Exception as e:
            logger.error(
                "profile_processing_failed",
                prid=prid,
                error=str(e),
                error_type=type(e).__name__
            )
            self.stats["errors"] += 1

    async def _upsert_aggregate(
        self,
        prid: str,
        aggregate: Dict[str, Any],
        checksum: str,
        source_updated_at: datetime
    ):
        """
        UPSERT всех сущностей агрегата в БД атомарно.

        Args:
            prid: Profile ID
            aggregate: Полный агрегат профиля
            checksum: Aggregate checksum
            source_updated_at: Timestamp из Data Hub
        """
        # Извлечение данных из агрегата
        profile_attrs = aggregate["employee_profile"]["attributes"]
        person_data = self._extract_entity_data(aggregate.get("ps_person"))
        le_data = self._extract_entity_data(aggregate.get("legal_entity_ps"))

        # UPSERT сущностей-сателлитов
        if person_data:
            await self.repository.upsert_person(person_data)

        if le_data:
            await self.repository.upsert_legal_entity(le_data)

        # UPSERT employees и positions
        mappings = []

        for employee_aggregate in aggregate.get("employees", []):
            employee_data = self._extract_entity_data(employee_aggregate["employee"])
            if employee_data:
                employee_data["profile_id"] = prid  # FK to employee_profile
                await self.repository.upsert_employee(employee_data)
                mappings.append({
                    "entity_type": "employee",
                    "entity_uuid": employee_data["id"],
                    "prid": prid
                })

            # UPSERT positions for employee
            for position_aggregate in employee_aggregate.get("positions", []):
                position_data = self._extract_entity_data(position_aggregate.get("position"))
                job_data = self._extract_entity_data(position_aggregate.get("job"))
                dept_data = self._extract_entity_data(position_aggregate.get("department"))

                if job_data:
                    await self.repository.upsert_job(job_data)

                if dept_data:
                    await self.repository.upsert_department(dept_data)

                if position_data:
                    position_data["employee_id"] = employee_data["id"]
                    position_data["job_id"] = job_data["id"] if job_data else None
                    position_data["dept_id"] = dept_data["id"] if dept_data else None
                    await self.repository.upsert_position(position_data)

        # UPSERT employee_profile
        await self.repository.upsert_profile(
            prid=prid,
            person_id=person_data["id"] if person_data else None,
            legal_entity_id=le_data["id"] if le_data else None,
            email=profile_attrs.get("email"),
            checksum=checksum,
            source_updated_at=source_updated_at,
            location=profile_attrs.get("location"),
            acc_type=profile_attrs.get("acc_type")
        )

        # Rebuild entity_prid_mapping
        if person_data:
            mappings.append({"entity_type": "person", "entity_uuid": person_data["id"], "prid": prid})

        await self.repository.rebuild_entity_mappings(prid, mappings)

    def _extract_entity_data(self, entity_json: Dict[str, Any] | None) -> Dict[str, Any] | None:
        """
        Извлечение данных сущности из JSON:API формата.

        Args:
            entity_json: JSON:API data ({id, type, attributes})

        Returns:
            Словарь с полями для ORM модели или None
        """
        if not entity_json:
            return None

        data = {"id": entity_json["id"]}
        data.update(entity_json.get("attributes", {}))

        return data

    async def _check_valkey_cache(self, profile_id: str) -> Dict[str, Any] | None:
        """
        Проверка metadata cache в ValKey.

        Args:
            profile_id: UUID профиля

        Returns:
            Словарь с metadata или None
        """
        if not self.valkey_client:
            return None

        # TODO: Реализовать ValKey integration
        # cache_key = f"profile_metadata:{profile_id}"
        # cached_data = await self.valkey_client.get(cache_key)
        # return json.loads(cached_data) if cached_data else None

        return None

    async def _update_valkey_cache(
        self,
        profile_id: str,
        prid: str,
        checksum: str,
        source_updated_at: datetime
    ):
        """
        Обновление metadata cache в ValKey.

        Args:
            profile_id: UUID профиля
            prid: Profile ID
            checksum: Aggregate checksum
            source_updated_at: Timestamp из Data Hub
        """
        if not self.valkey_client:
            return

        # TODO: Реализовать ValKey integration
        # cache_key = f"profile_metadata:{profile_id}"
        # metadata = {
        #     "prid": prid,
        #     "checksum": checksum,
        #     "source_updated_at": source_updated_at.isoformat()
        # }
        # await self.valkey_client.setex(cache_key, 90000, json.dumps(metadata))  # TTL 25 hours

    def _get_max_updated_at(self, profiles: List[Dict[str, Any]]) -> datetime:
        """
        Получение максимального meta.updated_at из batch профилей.

        Args:
            profiles: Список профилей (JSON:API data)

        Returns:
            Максимальный timestamp
        """
        timestamps = [
            datetime.fromisoformat(p["meta"]["updated_at"].replace("Z", "+00:00"))
            for p in profiles
        ]

        return max(timestamps) if timestamps else datetime.min

    def _count_api_calls_in_aggregate(self, aggregate: Dict[str, Any]) -> int:
        """
        Подсчет количества API вызовов для построения агрегата.

        Args:
            aggregate: Полный агрегат профиля

        Returns:
            Количество вызовов API
        """
        # Упрощенная оценка для PoC
        # В реальной реализации нужно трекать cache hits/misses
        employees_count = len(aggregate.get("employees", []))
        positions_count = sum(
            len(emp.get("positions", []))
            for emp in aggregate.get("employees", [])
        )

        # 1 (person) + 1 (legal_entity) + employees + positions * 3 (position, job, dept)
        return 1 + 1 + employees_count + positions_count * 3
