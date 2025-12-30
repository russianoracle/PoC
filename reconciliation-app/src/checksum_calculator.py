"""
Checksum Calculator для вычисления Aggregate Checksum профилей.

Вычисляет SHA256 от канонического JSON агрегата профиля для детектирования
изменений в ЛЮБОЙ сущности агрегата (employee_profile, ps_person, ps_job и т.д.).
"""

import hashlib
import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class ChecksumCalculator:
    """
    Калькулятор контрольных сумм для агрегатов профилей.

    Реализует алгоритм Aggregate Checksum из архитектуры eFlow:
    1. Сериализация агрегата в канонический JSON (sorted keys)
    2. Исключение timestamps (meta.created_at, meta.updated_at)
    3. SHA256 от UTF-8 строки
    """

    # Поля с timestamps, которые исключаются из checksum
    EXCLUDED_FIELDS = {
        "created_at",
        "updated_at",
        "meta"
    }

    @classmethod
    def calculate(cls, aggregate: Dict[str, Any]) -> str:
        """
        Вычисление aggregate checksum.

        Args:
            aggregate: Словарь с полным агрегатом профиля
                      (результат AggregateBuilder.build_aggregate)

        Returns:
            SHA256 checksum (hex строка, 64 символа)
        """
        # Шаг 1: Удаление timestamps из агрегата
        cleaned_aggregate = cls._remove_timestamps(aggregate)

        # Шаг 2: Сериализация в канонический JSON
        canonical_json = json.dumps(
            cleaned_aggregate,
            sort_keys=True,  # Детерминированный порядок ключей
            ensure_ascii=False
        )

        # Шаг 3: Вычисление SHA256
        checksum = hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()

        logger.debug(
            "checksum_calculated",
            profile_id=aggregate.get("employee_profile", {}).get("id"),
            checksum=checksum[:16] + "..."
        )

        return checksum

    @classmethod
    def _remove_timestamps(cls, data: Any) -> Any:
        """
        Рекурсивное удаление timestamp полей из структуры данных.

        Args:
            data: Словарь, список или примитивное значение

        Returns:
            Очищенная структура данных без timestamps
        """
        if isinstance(data, dict):
            return {
                key: cls._remove_timestamps(value)
                for key, value in data.items()
                if key not in cls.EXCLUDED_FIELDS
            }
        elif isinstance(data, list):
            return [cls._remove_timestamps(item) for item in data]
        else:
            return data

    @classmethod
    def verify_checksum(cls, aggregate: Dict[str, Any], expected_checksum: str) -> bool:
        """
        Проверка checksum агрегата.

        Args:
            aggregate: Словарь с агрегатом профиля
            expected_checksum: Ожидаемый checksum (из БД или кеша)

        Returns:
            True если checksum совпадает, False иначе
        """
        actual_checksum = cls.calculate(aggregate)

        matches = actual_checksum == expected_checksum

        if not matches:
            logger.debug(
                "checksum_mismatch",
                profile_id=aggregate.get("employee_profile", {}).get("id"),
                expected=expected_checksum[:16] + "...",
                actual=actual_checksum[:16] + "..."
            )

        return matches
