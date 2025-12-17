"""
SQLAlchemy модели для нормализованной схемы профилей сотрудников (3NF).

Соответствует спецификации из employee-profile-normalized-schema.md
"""

import enum
from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    Column, String, Date, Boolean, ForeignKey,
    TIMESTAMP, Enum, Text, Integer
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


# Enums для типизированных полей

class LocationEnum(str, enum.Enum):
    """Тип местоположения сотрудника."""
    OFFICE = "Office"
    HYBRID = "Hybrid"
    REMOTE = "Remote"


class AccTypeEnum(str, enum.Enum):
    """Тип учетной записи."""
    STAFF = "Staff"
    OSP = "OSP"
    SERVICE = "Service"
    SYSTEM = "System"


class StatusEnum(str, enum.Enum):
    """Статус трудовых отношений."""
    WORKING = "Работает"
    DISMISSED = "Уволен"
    MATERNITY_LEAVE = "Декретный отпуск"
    VACATION = "Отпуск"
    SICK_LEAVE = "Больничный"


class WorkerEnum(str, enum.Enum):
    """Тип работника."""
    FIELD = "Полевой"
    BACK_OFFICE = "Бек-офисный"
    OTHER = "Прочее"


class GenderEnum(str, enum.Enum):
    """Пол."""
    MALE = "Мужской"
    FEMALE = "Женский"


# Модели таблиц


class Person(Base):
    """
    Персональные данные физических лиц.

    Хранит ФИО, дату рождения, контакты.
    """
    __tablename__ = "ps_persons"

    id = Column(UUID(as_uuid=True), primary_key=True)
    surname_rus = Column(String(100))
    name_rus = Column(String(100))
    patronymic_rus = Column(String(100))
    surname_eng = Column(String(100))
    name_eng = Column(String(100))
    patronymic_eng = Column(String(100))
    birth_date = Column(Date)
    gender = Column(Enum(GenderEnum))
    email = Column(String(255))
    phone = Column(String(50))
    created_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")
    updated_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")

    # Relationships
    profiles = relationship("EmployeeProfile", back_populates="person")
    managed_depts = relationship("Department", back_populates="manager")


class LegalEntity(Base):
    """
    Справочник юридических лиц.

    Хранит информацию о юрлицах (ИНН, КПП, ОГРН).
    """
    __tablename__ = "legal_entities_ps"

    id = Column(UUID(as_uuid=True), primary_key=True)
    name = Column(String(255), nullable=False)
    inn = Column(String(20))
    kpp = Column(String(20))
    ogrn = Column(String(20))
    type = Column(String(50))
    phone = Column(String(50))
    created_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")
    updated_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")

    # Relationships
    profiles = relationship("EmployeeProfile", back_populates="legal_entity")
    departments = relationship("Department", back_populates="legal_entity")


class EmployeeProfile(Base):
    """
    Профили сотрудников (основная таблица).

    Связывает персону с юридическим лицом и хранит метаданные профиля.
    Primary key: prid (уникальный идентификатор профиля)
    """
    __tablename__ = "employee_profiles"

    prid = Column(String(50), primary_key=True)
    person_id = Column(UUID(as_uuid=True), ForeignKey("ps_persons.id"), nullable=False)
    legal_entity_id = Column(UUID(as_uuid=True), ForeignKey("legal_entities_ps.id"))
    email = Column(String(255))
    location = Column(Enum(LocationEnum))
    acc_type = Column(Enum(AccTypeEnum))
    checksum = Column(String(64))  # SHA256 для reconciliation
    source_updated_at = Column(TIMESTAMP, nullable=False)
    deleted_at = Column(TIMESTAMP)  # Soft delete
    created_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")
    updated_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")

    # Relationships
    person = relationship("Person", back_populates="profiles")
    legal_entity = relationship("LegalEntity", back_populates="profiles")
    employees = relationship("Employee", back_populates="profile", cascade="all, delete-orphan")


class Employee(Base):
    """
    Трудовые отношения сотрудника.

    Один профиль может иметь несколько трудовых отношений (одновременно или последовательно).
    """
    __tablename__ = "employees"

    id = Column(UUID(as_uuid=True), primary_key=True)
    profile_id = Column(String(50), ForeignKey("employee_profiles.prid"), nullable=False)
    number = Column(String(50))  # Табельный номер
    date_hiring = Column(Date)
    date_dismissal = Column(Date)
    status = Column(Enum(StatusEnum))
    worker = Column(Enum(WorkerEnum))
    created_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")
    updated_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")

    # Relationships
    profile = relationship("EmployeeProfile", back_populates="employees")
    positions = relationship("Position", back_populates="employee", cascade="all, delete-orphan")
    functional_roles = relationship("EmployeeFunctionalRole", back_populates="employee", cascade="all, delete-orphan")


class Job(Base):
    """
    Справочник должностей.

    Справочная таблица для ps_positions.
    """
    __tablename__ = "ps_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    external_id = Column(String(100))
    created_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")
    updated_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")

    # Relationships
    positions = relationship("Position", back_populates="job")


class Department(Base):
    """
    Справочник подразделений.

    Иерархическая структура подразделений (parent_dept_id → self-reference).
    """
    __tablename__ = "ps_departments"

    id = Column(UUID(as_uuid=True), primary_key=True)
    legal_entity_id = Column(UUID(as_uuid=True), ForeignKey("legal_entities_ps.id"), nullable=False)
    parent_dept_id = Column(UUID(as_uuid=True), ForeignKey("ps_departments.id"))
    manager_person_id = Column(UUID(as_uuid=True), ForeignKey("ps_persons.id"))
    name = Column(String(255), nullable=False)
    description = Column(Text)
    business_unit = Column(String(100))
    hierarchy_type_id = Column(UUID(as_uuid=True))
    external_id = Column(String(100))
    date_from = Column(Date)
    date_to = Column(Date)
    created_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")
    updated_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")

    # Relationships
    legal_entity = relationship("LegalEntity", back_populates="departments")
    parent_dept = relationship("Department", remote_side=[id], backref="child_depts")
    manager = relationship("Person", back_populates="managed_depts")
    positions = relationship("Position", back_populates="department")


class Position(Base):
    """
    Штатные позиции сотрудников.

    Связывает employee с job и department, хранит детали позиции (grade, region, etc).
    """
    __tablename__ = "ps_positions"

    id = Column(UUID(as_uuid=True), primary_key=True)
    employee_id = Column(UUID(as_uuid=True), ForeignKey("employees.id"), nullable=False)
    job_id = Column(UUID(as_uuid=True), ForeignKey("ps_jobs.id"), nullable=False)
    dept_id = Column(UUID(as_uuid=True), ForeignKey("ps_departments.id"))
    parent_position_id = Column(UUID(as_uuid=True), ForeignKey("ps_positions.id"))
    name = Column(String(255))
    type = Column(String(50))
    grade = Column(String(50))
    region = Column(String(100))
    business_unit = Column(String(100))
    field_force_group = Column(String(100))
    therapeutic_area = Column(String(100))
    car_grade_plan = Column(String(50))
    car_grade_fact = Column(String(50))
    is_manager = Column(Boolean, default=False)
    function = Column(String(100))
    po = Column(String(100))
    supplier_service = Column(String(255))
    supplier_legal_entity = Column(String(255))
    date_from = Column(Date)
    date_to = Column(Date)
    created_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")
    updated_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")

    # Relationships
    employee = relationship("Employee", back_populates="positions")
    job = relationship("Job", back_populates="positions")
    department = relationship("Department", back_populates="positions")
    parent_position = relationship("Position", remote_side=[id], backref="child_positions")


class FunctionalRole(Base):
    """
    Справочник функциональных ролей.

    Справочная таблица для employee_functional_roles.
    """
    __tablename__ = "ps_functional_roles"

    id = Column(UUID(as_uuid=True), primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    code = Column(String(50))
    external_id = Column(String(100))
    created_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")
    updated_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")

    # Relationships
    employees = relationship("EmployeeFunctionalRole", back_populates="functional_role")


class EmployeeFunctionalRole(Base):
    """
    Связь many-to-many между employees и functional_roles.

    Composite primary key: (employee_id, functional_role_id, date_from)
    """
    __tablename__ = "employee_functional_roles"

    employee_id = Column(UUID(as_uuid=True), ForeignKey("employees.id"), primary_key=True)
    functional_role_id = Column(UUID(as_uuid=True), ForeignKey("ps_functional_roles.id"), primary_key=True)
    date_from = Column(Date, primary_key=True, nullable=False)
    date_to = Column(Date)
    created_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")

    # Relationships
    employee = relationship("Employee", back_populates="functional_roles")
    functional_role = relationship("FunctionalRole", back_populates="employees")


class EntityPridMapping(Base):
    """
    Inverse index для маппинга UUID → PRID.

    Используется для поиска профиля по UUID любой связанной сущности
    (person, employee, position, etc).

    Пример: GET /profiles?filter[person_uuid]=11111111-1111-1111-1111-111111111111
    → SELECT prid FROM entity_prid_mapping WHERE entity_type='person' AND entity_uuid='11111111-...'
    → SELECT * FROM employee_profiles WHERE prid='enp6ejAwMA'
    """
    __tablename__ = "entity_prid_mapping"

    entity_type = Column(String(50), primary_key=True)  # person, employee, position, etc
    entity_uuid = Column(UUID(as_uuid=True), primary_key=True)
    prid = Column(String(50), nullable=False, index=True)
    created_at = Column(TIMESTAMP, nullable=False, server_default="NOW()")