# Reconciliation PoC для интеграции с Data Hub

## Обзор

Proof-of-Concept приложение для синхронизации профилей сотрудников из Data Hub REST API в нормализованную схему PostgreSQL.

**Ключевые особенности:**

- **Reactive Resilience:** Circuit Breaker pattern для защиты от cascading failures
- **Cloud Native:** 12-Factor конфигурация, health/readiness probes, graceful shutdown
- **SOLID:** Dependency Injection через IoC Container, разделение ответственностей
- Универсальный механизм квотирования через `QuotaManager` (Token Bucket алгоритм)
- Нормализованная схема 3NF (10 таблиц PostgreSQL)
- Checksum-based сверка данных для определения изменений
- In-memory кеш справочников (снижение с 9000 до 40 HTTP requests/hour)
- Kubernetes CronJob ready с конфигурацией secrets/configmaps
- Полное соответствие OpenAPI спецификации профиля

---

## Архитектура PoC

### Компонентная диаграмма

![Component Diagram](https://www.plantuml.com/plantuml/proxy?src=https://raw.githubusercontent.com/russianoracle/PoC/refs/heads/authorities-dictionary/reconciliation-app/component-diagram.puml)

**Ключевые компоненты:**

### Reactive & Resilience

1. **CircuitBreaker** ([circuit_breaker.py](src/circuit_breaker.py))
   - Три состояния: CLOSED, OPEN, HALF_OPEN
   - Автоматическое восстановление после timeout
   - Защита от cascading failures при проблемах с Data Hub API

2. **QuotaManager** ([quota_manager.py](src/quota_manager.py))
   - Token Bucket алгоритм через ValKey
   - Универсальный для всех 4 доменов
   - Предотвращение перегрузки Data Hub API

### Cloud Native & SOLID

3. **ConfigLoader** ([config.py](src/config.py))
   - 12-Factor конфигурация из environment variables
   - Typed configuration через dataclasses
   - Валидация обязательных параметров (SRP)

4. **ServiceContainer** ([container.py](src/container.py))
   - IoC Container для управления зависимостями (DIP)
   - Lazy initialization сервисов
   - Graceful shutdown с cleanup

### Authorization & HTTP

5. **ZitadelAuthService** ([auth_service.py](src/auth_service.py))
   - OAuth 2.0 Client Credentials flow
   - Кеширование JWT токенов в ValKey (TTL 55 минут)
   - Автоматический refresh токенов

6. **AuthenticatedHTTPClient** ([auth_service.py](src/auth_service.py))
   - Автоматическая авторизация всех HTTP запросов
   - Circuit Breaker интеграция для Reactive resilience
   - Логика повторов при HTTP 401 Unauthorized
   - Fail fast при HTTP 403 Forbidden

### Data Layer (Repository Pattern)

7. **DatabaseSessionManager** ([database.py](src/database.py))
   - SQLAlchemy 2.0 async session factory
   - Connection pool management (asyncpg)
   - Graceful shutdown для БД соединений

8. **UnitOfWork** ([unit_of_work.py](src/unit_of_work.py))
   - Управление транзакциями (ACID)
   - Context manager для commit/rollback
   - Координация репозиториев

9. **ProfileRepository** ([repository.py](src/repository.py))
   - CRUD операции через ORM
   - PostgreSQL UPSERT (on_conflict_do_update)
   - Checksum-based reconciliation queries
   - Bulk operations для initial load

10. **SQLAlchemy Models** ([models.py](src/models.py))
    - 10 нормализованных таблиц (3NF)
    - Foreign key constraints
    - Enum types для типизации

---

### Механизм квотирования (ключевая фича)

**Проблема:** 4 домена в Фазе 1 создают нагрузку ~160 requests/hour на Data Hub API.

**Решение:** `QuotaManager` с Token Bucket алгоритмом через Redis/ValKey.

```python
from src.quota_manager import QuotaManager, DataHubQuotaConfig

# Инициализация для домена "profiles"
config = DataHubQuotaConfig.get_config("profiles")
quota_mgr = QuotaManager(
    redis_client=redis_client,
    domain="profiles",
    max_requests_per_hour=config["max_requests_per_hour"],
    burst_size=config["burst_size"]
)

# Использование перед HTTP запросом
async with quota_mgr.acquire():
    response = await http_client.get("/employee-profiles")
```

**Staggered schedule для 4 доменов:**

| Домен            | CronJob Schedule | Max requests/hour |
| ---------------- | ---------------- | ----------------- |
| Profiles         | `0 * * * *`      | 50                |
| Authorities      | `15 * * * *`     | 50                |
| Signatures       | `30 * * * *`     | 50                |
| Document Signing | `45 * * * *`     | 50                |

**Эффект:** Peak load ~40 requests/burst каждые 15 минут (вместо одновременных 160 requests).

---

## Конфигурация: Секреты и переменные окружения

**ВАЖНО:** Все секреты хранятся в Yandex Cloud KMS, переменные окружения в Lockbox.

### Секреты (хранятся в Yandex Cloud Lockbox)

| Переменная              | Тип    | Обязательная | Описание                      | Пример значения                      |
| ----------------------- | ------ | ------------ | ----------------------------- | ------------------------------------ |
| `DATA_HUB_API_URL`      | URL    | Да           | Базовый URL Data Hub REST API | `https://datahub.example.com`        |
| `POSTGRES_HOST`         | Строка | Да           | Хост PostgreSQL               | `<MANAGED_POSTGRES_HOST>`            |
| `POSTGRES_PASSWORD`     | Секрет | Да           | Пароль PostgreSQL             | `<strong_password>`                  |
| `VALKEY_HOST`           | Строка | Да           | Хост ValKey (Redis)           | `<MANAGED_VALKEY_HOST>`              |
| `VALKEY_PASSWORD`       | Секрет | Да           | Пароль ValKey                 | `<strong_password>`                  |
| `ZITADEL_ISSUER`        | URL    | Да           | URL Zitadel OIDC instance     | `https://zitadel.example.com`        |
| `ZITADEL_CLIENT_ID`     | Строка | Да           | Service Account Client ID     | `<CLIENT_ID>@reconciliation-service` |
| `ZITADEL_CLIENT_SECRET` | Секрет | Да           | Service Account Client Secret | `<generated_secret>`                 |
| `ZITADEL_SCOPES`        | Строка | Да           | OAuth 2.0 scopes для Data Hub | `data_hub:people_service`            |

### Переменные окружения (non-sensitive)

| Переменная                    | Тип     | Значение по умолчанию                                        | Описание                                                                              |
| ----------------------------- | ------- | ------------------------------------------------------------ | ------------------------------------------------------------------------------------- |
| `DOMAIN`                      | Строка  | `profiles`                                                   | Домен для сверки данных (`profiles`, `authorities`, `signatures`, `document_signing`) |
| `POSTGRES_PORT`               | Число   | `5432`                                                       | Порт PostgreSQL                                                                       |
| `POSTGRES_DB`                 | Строка  | `eflow_profiles`                                             | Название базы данных                                                                  |
| `POSTGRES_USER`               | Строка  | `reconciliation_user`                                        | PostgreSQL пользователь                                                               |
| `POSTGRES_SSL_MODE`           | Строка  | `require`                                                    | Режим SSL для PostgreSQL                                                              |
| `VALKEY_PORT`                 | Число   | `6379`                                                       | Порт ValKey                                                                           |
| `VALKEY_SSL`                  | Boolean | `true`                                                       | Использовать SSL для ValKey                                                           |
| `PAGE_SIZE`                   | Число   | `100`                                                        | Размер страницы для Data Hub API                                                      |
| `RECONCILIATION_TIMEOUT`      | Число   | `600`                                                        | Таймаут сверки данных в секундах                                                      |
| `LOG_LEVEL`                   | Строка  | `INFO`                                                       | Уровень логирования (`DEBUG`, `INFO`, `WARNING`, `ERROR`)                             |
| `LOG_FORMAT`                  | Строка  | `json`                                                       | Формат логов (`json`, `text`)                                                         |
| `DATA_HUB_API_TIMEOUT`        | Число   | `30`                                                         | Таймаут HTTP запросов к Data Hub в секундах                                           |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | URL     | `http://otel-collector.observability.svc.cluster.local:4317` | OpenTelemetry Collector endpoint                                                      |
| `OTEL_SERVICE_NAME`           | Строка  | `profile-reconciliation`                                     | Имя сервиса для OpenTelemetry                                                         |
| `OTEL_RESOURCE_ATTRIBUTES`    | Строка  | `service.version=v1.3.1,deployment.environment=production`   | Атрибуты ресурса для OpenTelemetry                                                    |

### Получение секретов из Yandex Cloud Lockbox

**Для локальной разработки:**

```bash
# Получить секреты из Lockbox
yc lockbox payload get \
  --name reconciliation-secrets \
  --format json | jq -r '.entries[] | "export \(.key)=\(.text_value)"'
```

**Для Kubernetes:**

Используйте External Secrets Operator для автоматической синхронизации секретов из Lockbox в K8s Secrets (см. раздел "Kubernetes deploy с Yandex Cloud Lockbox").

---

## Алгоритм сверки данных (5 этапов)

### Этап 1: Загрузка данных из Data Hub API

```python
# Incremental sync (только измененные профили)
response = await http_client.get(
    "/employee-profiles",
    params={
        "filter[updated_at][gte]": last_run_timestamp.isoformat(),
        "page[limit]": 100,
        "page[offset]": 0
    }
)
```

**Оптимизация:** Параллельная предзагрузка справочников (jobs, departments, functional_roles) → снижение HTTP requests с 9000 до ~40/hour.

### Этап 2: Идентификация изменений

```python
def _calculate_checksum(profile_data: Dict) -> str:
    """SHA256 checksum для определения изменений."""
    normalized = json.dumps(profile_data, sort_keys=True)
    data_with_timestamp = f"{normalized}:{profile_data['meta']['updated_at']}"
    return hashlib.sha256(data_with_timestamp.encode()).hexdigest()
```

**Эффект:** Пропуск 99% неизменных профилей (hourly sync обновляет только 30-300 из 3000).

### Этап 3: Трансформация (JSON:API → ORM)

```python
# Преобразование Data Hub JSON:API в SQLAlchemy модели
class ProfileTransformer:
    def transform_profile(self, profile_data: Dict) -> Dict[str, Any]:
        person = self._transform_person(profile_data["relationships"]["person"]["data"])
        legal_entity = self._transform_legal_entity(...)
        employee_profile = self._transform_employee_profile(...)
        employees = [self._transform_employee(...) for emp in ...]
        return {"person": person, "employee_profile": employee_profile, ...}
```

### Этап 4: Обновление БД (UPSERT в 7-8 таблиц)

```sql
BEGIN TRANSACTION;

-- 1. ps_persons
INSERT INTO ps_persons (id, surname_rus, name_rus, ...) VALUES (...)
ON CONFLICT (id) DO UPDATE SET surname_rus = EXCLUDED.surname_rus, ...;

-- 2. legal_entities_ps
INSERT INTO legal_entities_ps (id, name, inn, ...) VALUES (...)
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, ...;

-- 3. employee_profiles
INSERT INTO employee_profiles (prid, person_id, legal_entity_id, checksum, ...) VALUES (...)
ON CONFLICT (prid) DO UPDATE SET checksum = EXCLUDED.checksum, ...;

-- 4-7. employees, ps_positions, employee_functional_roles, entity_prid_mapping
...

COMMIT;
```

**Критично:** Все операции для профиля в **одной транзакции** для атомарности.

### Этап 5: Инвалидация кеша

```python
await cache_service.invalidate_profile(prid)  # DELETE prid:{prid} from ValKey
```

---

## Критические точки логирования

### 1. Старт сверки данных

```json
{
  "timestamp": "2025-12-17T12:00:00Z",
  "level": "INFO",
  "message": "Reconciliation started",
  "domain": "profiles",
  "mode": "incremental",
  "last_run_timestamp": "2025-12-17T11:00:00Z",
  "total_profiles": 3000
}
```

### 2. Квотирование (каждый HTTP request)

```json
{
  "timestamp": "2025-12-17T12:00:05Z",
  "level": "DEBUG",
  "message": "Token acquired",
  "domain": "profiles",
  "cost": 1,
  "remaining_tokens": 9.5,
  "retry_count": 0
}
```

### 3. Обработка профиля

```json
{
  "timestamp": "2025-12-17T12:00:10Z",
  "level": "INFO",
  "message": "Profile updated",
  "prid": "enp6ejAwMA",
  "old_checksum": "a1b2c3...",
  "new_checksum": "d4e5f6...",
  "changed": true,
  "duration_ms": 150
}
```

### 4. Database transaction

```json
{
  "timestamp": "2025-12-17T12:00:10.150Z",
  "level": "DEBUG",
  "message": "Transaction committed",
  "prid": "enp6ejAwMA",
  "tables_updated": ["ps_persons", "employee_profiles", "employees", "ps_positions"],
  "queries_count": 15,
  "duration_ms": 50
}
```

### 5. Ошибки

```json
{
  "timestamp": "2025-12-17T12:00:15Z",
  "level": "ERROR",
  "message": "Profile validation failed",
  "prid": "enp6ejAwMA",
  "errors": ["Email format invalid: invalid@"],
  "profile_data": {...}
}
```

### 6. Итоговые метрики

```json
{
  "timestamp": "2025-12-17T12:08:30Z",
  "level": "INFO",
  "message": "Reconciliation completed",
  "domain": "profiles",
  "duration_seconds": 510,
  "metrics": {
    "processed": 3000,
    "updated": 150,
    "skipped": 2840,
    "errors": 10
  },
  "http_requests": 42,
  "database_queries": 2400,
  "cache_hit_rate": 0.92,
  "quota_tokens_remaining": 8.2
}
```

---

## Структура проекта

```
poc/reconciliation-app/
├── README.md                          # Эта документация
├── component-diagram.puml             # PlantUML диаграмма компонентов
├── Dockerfile                         # Multi-stage Docker build
├── requirements.txt                   # Python dependencies
├── kubernetes/
│   ├── cronjob.yaml                  # K8s CronJob (schedule: 0 * * * *)
│   └── external-secret.yaml          # ExternalSecret для Yandex Cloud Lockbox
├── src/
│   ├── __init__.py
│   ├── main.py                       # Entrypoint (с DI и graceful shutdown)
│   ├── config.py                     # ConfigLoader (12-Factor, SRP)
│   ├── container.py                  # ServiceContainer (IoC, DIP)
│   ├── circuit_breaker.py            # Circuit Breaker (Reactive resilience)
│   ├── auth_service.py               # Zitadel OIDC авторизация
│   ├── quota_manager.py              # Token Bucket квотирование
│   ├── database.py                   # DatabaseSessionManager (async sessions)
│   ├── unit_of_work.py               # UnitOfWork pattern (транзакции)
│   ├── repository.py                 # ProfileRepository (data access)
│   └── models.py                     # SQLAlchemy модели (10 таблиц 3NF)
└── tests/
    └── test_quota_manager.py         # Unit-тесты QuotaManager
```

---

## Установка и запуск

### Локальная разработка

```bash
# 1. Установка зависимостей
pip install -r requirements.txt

# 2. Настройка окружения (получить из Yandex Cloud Lockbox)
export DATA_HUB_API_URL="<DATA_HUB_URL>"
export POSTGRES_HOST="<POSTGRES_HOST>"
export POSTGRES_DB="eflow_profiles"
export POSTGRES_USER="reconciliation_user"
export POSTGRES_PASSWORD="<secret>"
export VALKEY_HOST="<VALKEY_HOST>"
export VALKEY_PASSWORD="<secret>"
export ZITADEL_ISSUER="<ZITADEL_ISSUER>"
export ZITADEL_CLIENT_ID="<CLIENT_ID>"
export ZITADEL_CLIENT_SECRET="<CLIENT_SECRET>"
export ZITADEL_SCOPES="data_hub:people_service"
export DOMAIN="profiles"

# 3. Запуск
python src/main.py
```

### Docker build

```bash
# Multi-stage build (оптимизация размера образа)
docker build -t reconciliation-app:latest .

# Запуск
docker run --env-file .env reconciliation-app:latest
```

### Kubernetes deploy с Yandex Cloud Lockbox

**ВАЖНО:** Все секреты хранятся в Yandex Cloud KMS, переменные окружения в Lockbox.

#### Шаг 1: Создание секретов в Yandex Cloud Lockbox

Создайте Lockbox secret с следующими ключами:

```bash
# Создание секрета для reconciliation service
yc lockbox secret create \
  --name reconciliation-secrets \
  --description "Секреты для Reconciliation CronJob" \
  --payload \
    '[
      {"key":"DATA_HUB_API_URL","text_value":"<DATA_HUB_URL>"},
      {"key":"POSTGRES_HOST","text_value":"<POSTGRES_HOST>"},
      {"key":"POSTGRES_USER","text_value":"reconciliation_user"},
      {"key":"POSTGRES_PASSWORD","text_value":"<POSTGRES_PASSWORD>"},
      {"key":"VALKEY_HOST","text_value":"<VALKEY_HOST>"},
      {"key":"VALKEY_PASSWORD","text_value":"<VALKEY_PASSWORD>"},
      {"key":"ZITADEL_ISSUER","text_value":"<ZITADEL_ISSUER>"},
      {"key":"ZITADEL_CLIENT_ID","text_value":"<CLIENT_ID>"},
      {"key":"ZITADEL_CLIENT_SECRET","text_value":"<CLIENT_SECRET>"},
      {"key":"ZITADEL_SCOPES","text_value":"data_hub:people_service"}
    ]'
```

#### Шаг 2: Настройка External Secrets Operator

Установите External Secrets Operator в кластер:

```bash
helm repo add external-secrets https://charts.external-secrets.io
helm install external-secrets \
  external-secrets/external-secrets \
  -n external-secrets-system \
  --create-namespace
```

Создайте SecretStore для Yandex Cloud Lockbox:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: SecretStore
metadata:
  name: yc-lockbox-store
  namespace: eflow-prod
spec:
  provider:
    yandexlockbox:
      apiEndpoint: lockbox.api.cloud.yandex.net:443
      auth:
        authorizedKey:
          secretRef:
            name: yc-sa-key
            key: authorized-key
```

Создайте ExternalSecret для синхронизации из Lockbox:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: reconciliation-external-secret
  namespace: eflow-prod
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: yc-lockbox-store
    kind: SecretStore
  target:
    name: reconciliation-secrets
    creationPolicy: Owner
  data:
    - secretKey: data_hub_url
      remoteRef:
        key: reconciliation-secrets
        property: DATA_HUB_API_URL
    - secretKey: postgres_host
      remoteRef:
        key: reconciliation-secrets
        property: POSTGRES_HOST
    - secretKey: postgres_user
      remoteRef:
        key: reconciliation-secrets
        property: POSTGRES_USER
    - secretKey: postgres_password
      remoteRef:
        key: reconciliation-secrets
        property: POSTGRES_PASSWORD
    - secretKey: valkey_host
      remoteRef:
        key: reconciliation-secrets
        property: VALKEY_HOST
    - secretKey: valkey_password
      remoteRef:
        key: reconciliation-secrets
        property: VALKEY_PASSWORD
    - secretKey: zitadel_issuer
      remoteRef:
        key: reconciliation-secrets
        property: ZITADEL_ISSUER
    - secretKey: zitadel_client_id
      remoteRef:
        key: reconciliation-secrets
        property: ZITADEL_CLIENT_ID
    - secretKey: zitadel_client_secret
      remoteRef:
        key: reconciliation-secrets
        property: ZITADEL_CLIENT_SECRET
    - secretKey: zitadel_scopes
      remoteRef:
        key: reconciliation-secrets
        property: ZITADEL_SCOPES
```

#### Шаг 3: Применение конфигурации

```bash
# 1. Создание namespace
kubectl create namespace eflow-prod

# 2. Применение ExternalSecret
kubectl apply -f kubernetes/external-secret.yaml

# 3. Применение CronJob
kubectl apply -f kubernetes/cronjob.yaml

# 4. Мониторинг
kubectl get cronjobs -n eflow-prod
kubectl get jobs -n eflow-prod
kubectl logs -f job/profile-reconciliation-<timestamp> -n eflow-prod
```

**Проверка синхронизации секретов:**

```bash
# Проверить что ExternalSecret создал K8s Secret
kubectl get secret reconciliation-secrets -n eflow-prod

# Проверить ключи в секрете
kubectl get secret reconciliation-secrets -n eflow-prod -o jsonpath='{.data}' | jq 'keys'
```

---

## Использование QuotaManager в других доменах

### Пример: Сверка данных для Authorities

```python
from src.quota_manager import QuotaManager, DataHubQuotaConfig

# Конфигурация для домена "authorities"
config = DataHubQuotaConfig.get_config("authorities")

# Инициализация QuotaManager
quota_mgr = QuotaManager(
    redis_client=redis_client,
    domain="authorities",  # ← Другой домен
    max_requests_per_hour=config["max_requests_per_hour"],
    burst_size=config["burst_size"]
)

# Использование идентично
async with quota_mgr.acquire():
    response = await http_client.get("/authorities")
```

### Добавление нового домена

**Шаг 1:** Добавить конфигурацию в `DataHubQuotaConfig.DOMAIN_CONFIGS`:

```python
"new_domain": {
    "max_requests_per_hour": 50,
    "burst_size": 10,
    "cron_schedule": "55 * * * *"  # :55 минута (после других доменов)
}
```

**Шаг 2:** Создать CronJob с `schedule: "55 * * * *"`:

```yaml
spec:
  schedule: "55 * * * *"
  env:
    - name: DOMAIN
      value: "new_domain"
```

**Всё!** QuotaManager автоматически обрабатывает квотирование для нового домена через единый Redis bucket.

---

## Архитектурные принципы

### Reactive Manifesto

- **Responsive:** Circuit Breaker предотвращает cascading failures
- **Resilient:** Quota Manager защищает от перегрузки Data Hub API
- **Elastic:** Kubernetes CronJob масштабируется горизонтально
- **Message Driven:** Готовность к миграции на Kafka (Фаза 2)

### Cloud Native (12-Factor App)

- **Конфигурация:** Environment variables через Yandex Cloud Lockbox
- **Зависимости:** Explicit dependencies в requirements.txt
- **Процессы:** Stateless CronJob (состояние в PostgreSQL/ValKey)
- **Observability:** Structured JSON logs + OpenTelemetry tracing
- **Disposability:** Graceful shutdown с cleanup ресурсов
- **Dev/Prod parity:** Единый Docker образ для всех окружений

### SOLID Principles

- **SRP:** ConfigLoader отвечает только за конфигурацию, Repository — за data access
- **OCP:** Circuit Breaker расширяется через CircuitBreakerConfig
- **LSP:** Все сервисы реализуют единый lifecycle (init → work → cleanup)
- **ISP:** Узкие интерфейсы (QuotaManager, ProfileRepository, CircuitBreaker)
- **DIP:** ServiceContainer управляет зависимостями, UnitOfWork абстрагирует транзакции

### Repository Pattern

**Unit of Work + Repository** — разделение ответственностей:
- **UnitOfWork**: Управление транзакциями (begin, commit, rollback)
- **Repository**: Инкапсуляция SQL операций (CRUD, UPSERT, queries)
- **Testability**: Моки для unit-тестов (mockable UnitOfWork)
- **ORM abstraction**: Нет raw SQL в бизнес-логике

---

## Метрики успеха

| Метрика                      | Целевое значение | Комментарий                                       |
| ---------------------------- | ---------------- | ------------------------------------------------- |
| Duration                     | < 600s           | Timeout CronJob: 10 минут                         |
| Processed profiles           | ~3000            | Все профили                                       |
| Updated profiles             | 30-300           | Зависит от изменений (1-10%)                      |
| Skipped profiles             | 2700-2970        | Неизменные профили (checksum match)               |
| Errors                       | < 1%             | Допустимо до 30 ошибок                            |
| HTTP requests to Data Hub    | ~40/hour         | 30 страниц + 10 уникальных справочников (с кешем) |
| Cache hit rate               | > 90%            | ValKey кеш                                        |
| Database queries per profile | ~15-20           | UPSERT в 7-8 таблиц                               |
| Quota tokens remaining       | > 5              | Должно остаться токенов в bucket                  |

---

## Troubleshooting

### Проблема: "TimeoutError: Failed to acquire token"

**Причина:** Превышен rate limit Data Hub (10 req/sec) или другой домен исчерпал bucket.

**Решение:**
```bash
# Проверить оставшиеся токены
kubectl exec -it reconciliation-pod -n eflow-prod -- python -c "
from src.quota_manager import QuotaManager
import redis.asyncio as redis
import asyncio

async def check():
    r = await redis.from_url('rediss://...')
    qm = QuotaManager(r, 'profiles')
    remaining = await qm.get_remaining_tokens()
    print(f'Remaining tokens: {remaining}')

asyncio.run(check())
"
```

### Проблема: "PostgreSQL connection pool exhausted"

**Причина:** Слишком много параллельных заданий сверки.

**Решение:** Убедиться что `concurrencyPolicy: Forbid` в CronJob spec.

### Проблема: "Checksum mismatch after update"

**Причина:** Одновременное изменение профиля в Data Hub во время сверки данных.

**Решение:** Повторный запуск сверки данных (автоматически через CronJob hourly).

---

## Дальнейшее развитие

### Фаза 2: Миграция на Kafka

**Изменения:**
- Добавить `StreamConsumer` для обработки событий из Kafka
- Reconciliation переходит в резервный режим (hourly для восстановления пропущенных событий)
- QuotaManager остается без изменений (используется и для Kafka и для REST)

### Масштабирование на 4 домена

**Готовность:**
- QuotaManager поддерживает 4 домена через единый Redis
- Staggered schedule предотвращает peak load
- Модульная архитектура позволяет легко реплицировать для новых доменов

---

## Контакты

Для вопросов и предложений:
- Backend Team Lead: [контакт]
- Архитектор: [контакт]
- Документация: [Архитектура по релизам/1.3.1/architecture.md](../../Архитектура%20по%20релизам/1.3.1/architecture.md)
