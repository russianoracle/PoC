# Сервис сверки профилей сотрудников

## Обзор

Сервис синхронизации профилей сотрудников из Data Hub REST API в нормализованную схему PostgreSQL с оптимизацией нагрузки через трехуровневое кеширование и раннее прерывание.

**Ключевые особенности v1.3.0:**

- **Раннее прерывание:** Автоматическая остановка при отсутствии изменений (снижение нагрузки в 13.2x)
- **Трехуровневое кеширование:** In-Memory (справочники) → ValKey (метаданные) → PostgreSQL
- **Контрольная сумма агрегата:** SHA256 для детектирования изменений в ЛЮБОЙ сущности
- **Нормализованное хранилище:** 10 таблиц в 3NF (без JSONB для данных профилей)
- **Автоматический выключатель:** Защита от каскадных сбоев при проблемах с Data Hub
- **Облачно-ориентированная архитектура:** 12-Factor конфигурация, корректное завершение, OpenTelemetry
- **Kubernetes CronJob:** Ежечасная синхронизация с изоляцией по контурам

---

## Архитектура

### Основные модули v1.3.0

**Ядро сверки:**

1. **[ReconciliationService](src/reconciliation_service.py)** - Основная логика синхронизации
   - Раннее прерывание при `max(batch.updated_at) <= last_sync_timestamp`
   - Трехуровневое кеширование для минимизации запросов
   - Снижение нагрузки в 13.2x при 5% изменений

2. **[AggregateBuilder](src/aggregate_builder.py)** - Парсинг JSON:API
   - In-memory кеширование справочников (jobs: 99%, departments: 95%)
   - Построение полного агрегата профиля

3. **[ChecksumCalculator](src/checksum_calculator.py)** - Контрольные суммы
   - SHA256 для детектирования изменений
   - Исключение timestamps перед хешированием

**Слой данных:**

4. **[ProfileRepository](src/repository.py)** - Доступ к данным
   - UPSERT для всех сущностей-сателлитов
   - rebuild_entity_mappings() для inverse index

5. **[Models](src/models.py)** - ORM модели
   - 10 нормализованных таблиц (3NF)
   - Foreign key constraints

---

## Переменные окружения

### Секреты (Yandex Cloud Lockbox)

| Переменная              | Описание                        |
| ----------------------- | ------------------------------- |
| `DATA_HUB_API_URL`      | Базовый URL Data Hub REST API   |
| `POSTGRES_HOST`         | Хост PostgreSQL                 |
| `POSTGRES_PASSWORD`     | Пароль PostgreSQL               |
| `VALKEY_HOST`           | Хост ValKey                     |
| `VALKEY_PASSWORD`       | Пароль ValKey                   |
| `ZITADEL_ISSUER`        | URL Zitadel OIDC instance       |
| `ZITADEL_CLIENT_ID`     | Service Account Client ID       |
| `ZITADEL_CLIENT_SECRET` | Service Account Client Secret   |
| `ZITADEL_SCOPES`        | OAuth 2.0 scopes для Data Hub   |

### Конфигурация

| Переменная                    | По умолчанию | Описание                                                             |
| ----------------------------- | ------------ | -------------------------------------------------------------------- |
| `CONTOUR`                     | `prod`       | Контур развертывания (`dev`, `test`, `prod`)                         |
| `DOMAIN`                      | `profiles`   | Домен для сверки (`profiles`, `authorities`, `signatures`)          |
| `POSTGRES_PORT`               | `5432`       | Порт PostgreSQL                                                      |
| `POSTGRES_DB`                 | `eflow_profiles` | Название базы данных                                             |
| `POSTGRES_USER`               | `reconciliation_user` | PostgreSQL пользователь                                     |
| `VALKEY_PORT`                 | `6379`       | Порт ValKey                                                          |
| `VALKEY_DB`                   | `0`          | Номер БД ValKey (0=prod, 1=test, 2=dev)                             |
| `PAGE_SIZE`                   | `100`        | Размер страницы для Data Hub API                                    |
| `RECONCILIATION_TIMEOUT`      | `600`        | Тайм-аут сверки в секундах                                           |
| `LOG_LEVEL`                   | `INFO`       | Уровень логирования                                                  |
| `LOG_FORMAT`                  | `json`       | Формат логов                                                         |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector.observability.svc.cluster.local:4317` | OpenTelemetry Collector |
| `OTEL_SERVICE_NAME`           | `profile-reconciliation` | Имя сервиса для OpenTelemetry                           |

---

## Алгоритм сверки

### 1. Загрузка с ранним прерыванием

```python
# Запрос профилей с сортировкой по свежести
response = await http_client.get(
    "/employee-profiles",
    params={
        "sort": "-meta.updated_at",  # Свежие профили первыми
        "page[limit]": 100,
        "page[offset]": offset
    }
)

# Раннее прерывание
if max(batch.updated_at) <= last_sync_timestamp:
    break  # Все оставшиеся профили не изменились
```

### 2. Проверка изменений

**Уровень 1 - ValKey metadata cache:**
```python
cached = await valkey.get(f"profile_metadata:{uuid}")
if cached and cached["source_updated_at"] >= profile.updated_at:
    return  # SKIP - профиль не изменился
```

**Уровень 2 - Aggregate checksum:**
```python
aggregate = await build_aggregate(profile)  # С in-memory кешем справочников
new_checksum = sha256(canonical_json(aggregate))

if cached and cached["checksum"] == new_checksum:
    return  # SKIP - данные не изменились
```

### 3. Атомарное обновление

```sql
BEGIN TRANSACTION;

-- UPSERT сущностей-сателлитов
INSERT INTO ps_persons (...) ON CONFLICT (id) DO UPDATE ...;
INSERT INTO ps_jobs (...) ON CONFLICT (id) DO UPDATE ...;
INSERT INTO ps_departments (...) ON CONFLICT (id) DO UPDATE ...;
INSERT INTO employees (...) ON CONFLICT (id) DO UPDATE ...;
INSERT INTO ps_positions (...) ON CONFLICT (id) DO UPDATE ...;

-- UPSERT профиля
INSERT INTO employee_profiles (prid, person_id, checksum, ...)
ON CONFLICT (prid) DO UPDATE SET checksum = EXCLUDED.checksum, ...;

-- Перестроение entity_prid_mapping
DELETE FROM entity_prid_mapping WHERE prid = $1;
INSERT INTO entity_prid_mapping (entity_type, entity_uuid, prid) VALUES ...;

COMMIT;
```

---

## Структура проекта

```
reconciliation-app/
├── README.md
├── Dockerfile
├── requirements.txt
├── kubernetes/
│   ├── cronjob.yaml                  # Ежечасная синхронизация
│   └── external-secret.yaml          # Интеграция с Yandex Cloud Lockbox
├── src/
│   ├── reconciliation_service.py     # Основная логика сверки [v1.3.0]
│   ├── aggregate_builder.py          # Парсинг JSON:API [v1.3.0]
│   ├── checksum_calculator.py        # Контрольные суммы [v1.3.0]
│   ├── repository.py                 # Слой доступа к данным
│   ├── models.py                     # ORM модели (10 таблиц 3NF)
│   ├── database.py                   # Управление сессиями
│   ├── unit_of_work.py               # Управление транзакциями
│   ├── auth_service.py               # Zitadel OIDC авторизация
│   └── config.py                     # Конфигурация из env
└── tests/
    └── test_quota_manager.py
```

---

## Запуск

### Локальная разработка

```bash
# Установка зависимостей
pip install -r requirements.txt

# Настройка окружения
export DATA_HUB_API_URL="<DATA_HUB_URL>"
export POSTGRES_HOST="<HOST>"
export POSTGRES_PASSWORD="<PASSWORD>"
export VALKEY_HOST="<HOST>"
export VALKEY_PASSWORD="<PASSWORD>"
export ZITADEL_ISSUER="<ISSUER>"
export ZITADEL_CLIENT_ID="<CLIENT_ID>"
export ZITADEL_CLIENT_SECRET="<SECRET>"
export DOMAIN="profiles"
export CONTOUR="dev"

# Запуск
python src/main.py
```

### Docker

```bash
docker build -t reconciliation-app:latest .
docker run --env-file .env reconciliation-app:latest
```

### Kubernetes

```bash
# Создание namespace
kubectl create namespace eflow-prod

# Применение конфигурации
kubectl apply -f kubernetes/external-secret.yaml
kubectl apply -f kubernetes/cronjob.yaml

# Мониторинг
kubectl logs -f job/profile-reconciliation-<timestamp> -n eflow-prod
```

---

## Метрики производительности

| Метрика                    | Целевое значение | Описание                                 |
| -------------------------- | ---------------- | ---------------------------------------- |
| Длительность сверки        | < 600s           | Тайм-аут CronJob                         |
| Обработано профилей        | ~3000            | Все профили в базе                       |
| Обновлено профилей         | 30-300           | Зависит от % изменений                   |
| Пропущено по checksum      | 2700-2970        | Неизменные профили                       |
| HTTP запросов к Data Hub   | ~560/hour        | При 5% изменений (с ранним прерыванием)  |
| Попадание в кеш ValKey     | > 90%            | Metadata cache                           |
| SQL запросов на профиль    | ~15-20           | UPSERT в 7-8 таблиц                      |

### Эффективность кеширования

| Справочник     | Попадание в кеш | Эффект                          |
| -------------- | --------------- | ------------------------------- |
| Jobs           | 99%             | 1 запрос вместо 3000            |
| Departments    | 95%             | ~150 запросов вместо 3000       |
| Legal Entities | 99%             | 1-2 запроса вместо 3000         |

---

## Troubleshooting

### Тайм-аут получения токена

**Причина:** Превышен лимит Data Hub API или другой домен исчерпал квоту.

**Решение:** Проверить оставшиеся токены:
```bash
kubectl exec -it <pod> -- python -c "
from src.quota_manager import QuotaManager
# ... проверка remaining tokens
"
```

### Исчерпание пула соединений PostgreSQL

**Причина:** Параллельные задания сверки.

**Решение:** Убедиться что `concurrencyPolicy: Forbid` в CronJob.

### Несовпадение контрольной суммы

**Причина:** Одновременное изменение профиля во время сверки.

**Решение:** Автоматический повтор при следующем запуске (ежечасно).

---

## Дальнейшее развитие

### Фаза 2: Миграция на Kafka

- Добавить StreamConsumer для обработки событий
- Сверка переходит в резервный режим (восстановление пропущенных событий)
- Снижение задержки актуализации с 60 минут до ~5 секунд

### Масштабирование на 4 домена

- QuotaManager поддерживает 4 домена через единый ValKey
- Разнесенное расписание предотвращает пиковую нагрузку
- Модульная архитектура для быстрой репликации на новые домены