"""
Aggregate Builder для построения агрегатов профилей из JSON:API responses.

Парсит JSON:API формат Data Hub и строит полный агрегат профиля,
включая все связанные сущности (person, job, department, legal_entity, employee, position).
"""

import logging
from typing import Dict, Any, List, Optional
from uuid import UUID

logger = logging.getLogger(__name__)


class AggregateBuilder:
    """
    Строитель агрегатов профилей сотрудников.

    Использует in-memory кеш для справочников (job, department, legal_entity)
    для минимизации запросов к Data Hub API.
    """

    def __init__(self, http_client):
        """
        Инициализация AggregateBuilder.

        Args:
            http_client: HTTP клиент с circuit breaker для запросов к Data Hub
        """
        self.http_client = http_client

        # In-Memory кеш справочников (уровень 1 кеширования)
        self.job_cache = {}  # job_uuid → job_data
        self.department_cache = {}  # dept_uuid → dept_data
        self.legal_entity_cache = {}  # le_uuid → le_data
        self.person_cache = {}  # person_uuid → person_data (10% попадание)

    async def build_aggregate(self, profile_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        Построение полного агрегата профиля из JSON:API response.

        Args:
            profile_json: JSON:API response для employee-profile
                         (формат: {id, type, attributes, relationships})

        Returns:
            Словарь с полным агрегатом:
            {
                "employee_profile": {...},
                "ps_person": {...},
                "legal_entity_ps": {...},
                "employees": [{...}],
                "ps_positions": [{...}],
                "ps_jobs": [{...}],
                "ps_departments": [{...}]
            }
        """
        profile_id = profile_json["id"]
        attributes = profile_json["attributes"]
        relationships = profile_json.get("relationships", {})

        logger.debug("building_aggregate", profile_id=profile_id)

        aggregate = {
            "employee_profile": {
                "id": profile_id,
                "type": "employee-profile",
                "attributes": attributes
            }
        }

        # Получение связанных сущностей через JSON:API relationships

        # 1. Person
        if "person" in relationships:
            person_uuid = relationships["person"]["data"]["id"]
            aggregate["ps_person"] = await self._fetch_person(person_uuid)

        # 2. Legal Entity
        if "legal_entity_ps" in relationships:
            le_uuid = relationships["legal_entity_ps"]["data"]["id"]
            aggregate["legal_entity_ps"] = await self._fetch_legal_entity(le_uuid)

        # 3. Employees (может быть несколько)
        employees_data = []
        if "employees" in relationships:
            # Получаем employees через related link
            employees_link = relationships["employees"]["links"]["related"]
            employees_response = await self.http_client.get(employees_link)
            employees_json = employees_response.json()["data"]

            for employee_json in employees_json:
                employee_aggregate = await self._build_employee_aggregate(employee_json)
                employees_data.append(employee_aggregate)

        aggregate["employees"] = employees_data

        logger.debug("aggregate_built", profile_id=profile_id, employees_count=len(employees_data))

        return aggregate

    async def _fetch_person(self, person_uuid: str) -> Dict[str, Any]:
        """
        Получение персоны из Data Hub с кешированием.

        Args:
            person_uuid: UUID персоны

        Returns:
            Словарь с данными персоны
        """
        # Проверка in-memory кеша
        if person_uuid in self.person_cache:
            logger.debug("person_cache_hit", person_uuid=person_uuid)
            return self.person_cache[person_uuid]

        # Cache miss - запрос к Data Hub
        response = await self.http_client.get(f"/dictionaries/people-service/persons/{person_uuid}")
        person_data = response.json()["data"]

        # Сохранение в кеш
        self.person_cache[person_uuid] = person_data

        logger.debug("person_fetched", person_uuid=person_uuid)

        return person_data

    async def _fetch_legal_entity(self, le_uuid: str) -> Dict[str, Any]:
        """
        Получение юридического лица из Data Hub с кешированием.

        Args:
            le_uuid: UUID юр.лица

        Returns:
            Словарь с данными юр.лица
        """
        if le_uuid in self.legal_entity_cache:
            logger.debug("legal_entity_cache_hit", le_uuid=le_uuid)
            return self.legal_entity_cache[le_uuid]

        response = await self.http_client.get(f"/dictionaries/people-service/legal-entities/{le_uuid}")
        le_data = response.json()["data"]

        self.legal_entity_cache[le_uuid] = le_data
        logger.debug("legal_entity_fetched", le_uuid=le_uuid)

        return le_data

    async def _fetch_job(self, job_uuid: str) -> Dict[str, Any]:
        """
        Получение должности из Data Hub с кешированием (99% попадание).

        Args:
            job_uuid: UUID должности

        Returns:
            Словарь с данными должности
        """
        if job_uuid in self.job_cache:
            logger.debug("job_cache_hit", job_uuid=job_uuid)
            return self.job_cache[job_uuid]

        response = await self.http_client.get(f"/dictionaries/people-service/jobs/{job_uuid}")
        job_data = response.json()["data"]

        self.job_cache[job_uuid] = job_data
        logger.debug("job_fetched", job_uuid=job_uuid)

        return job_data

    async def _fetch_department(self, le_uuid: str, dept_uuid: str) -> Dict[str, Any]:
        """
        Получение подразделения из Data Hub с кешированием (95% попадание).

        Args:
            le_uuid: UUID юр.лица
            dept_uuid: UUID подразделения

        Returns:
            Словарь с данными подразделения
        """
        cache_key = f"{le_uuid}:{dept_uuid}"

        if cache_key in self.department_cache:
            logger.debug("department_cache_hit", dept_uuid=dept_uuid)
            return self.department_cache[cache_key]

        response = await self.http_client.get(
            f"/dictionaries/people-service/legal-entities/{le_uuid}/departments/{dept_uuid}"
        )
        dept_data = response.json()["data"]

        self.department_cache[cache_key] = dept_data
        logger.debug("department_fetched", dept_uuid=dept_uuid)

        return dept_data

    async def _build_employee_aggregate(self, employee_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        Построение агрегата для employee с его positions.

        Args:
            employee_json: JSON:API data для employee

        Returns:
            Словарь с employee и связанными positions
        """
        employee_id = employee_json["id"]
        relationships = employee_json.get("relationships", {})

        employee_aggregate = {
            "employee": {
                "id": employee_id,
                "type": "employee",
                "attributes": employee_json["attributes"]
            },
            "positions": []
        }

        # Получение positions для employee
        if "positions" in relationships:
            position_ids = [pos["id"] for pos in relationships["positions"]["data"]]

            for position_id in position_ids:
                position_data = await self._fetch_position(position_id)
                employee_aggregate["positions"].append(position_data)

        return employee_aggregate

    async def _fetch_position(self, position_id: str) -> Dict[str, Any]:
        """
        Получение position с job и department.

        Args:
            position_id: UUID позиции

        Returns:
            Словарь с position, job, department
        """
        response = await self.http_client.get(f"/dictionaries/people-service/positions/{position_id}")
        position_json = response.json()["data"]
        relationships = position_json.get("relationships", {})

        position_aggregate = {
            "position": {
                "id": position_id,
                "type": "ps_position",
                "attributes": position_json["attributes"]
            }
        }

        # Job
        if "job" in relationships:
            job_uuid = relationships["job"]["data"]["id"]
            position_aggregate["job"] = await self._fetch_job(job_uuid)

        # Department
        if "department" in relationships:
            dept_uuid = relationships["department"]["data"]["id"]
            # Предполагаем, что le_uuid известен из контекста
            # В реальной реализации нужно будет получить из parent aggregate
            le_uuid = relationships.get("legal_entity", {}).get("data", {}).get("id")
            if le_uuid:
                position_aggregate["department"] = await self._fetch_department(le_uuid, dept_uuid)

        return position_aggregate

    def clear_cache(self):
        """
        Очистка in-memory кеша справочников.

        Вызывается после завершения reconciliation session.
        """
        self.job_cache.clear()
        self.department_cache.clear()
        self.legal_entity_cache.clear()
        self.person_cache.clear()

        logger.debug("aggregate_builder_cache_cleared")
