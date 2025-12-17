# Multi-stage Docker build для оптимизации размера образа

# Stage 1: Builder
FROM python:3.11-slim as builder

WORKDIR /build

# Установка системных зависимостей
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Копирование requirements и установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Stage 2: Runtime
FROM python:3.11-slim

WORKDIR /app

# Установка runtime зависимостей
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Копирование Python packages из builder
COPY --from=builder /root/.local /root/.local

# Копирование исходного кода
COPY src/ ./src/

# Настройка PATH
ENV PATH=/root/.local/bin:$PATH

# Создание non-root пользователя
RUN useradd -m -u 1000 reconciliation && \
    chown -R reconciliation:reconciliation /app
USER reconciliation

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import sys; sys.exit(0)"

# Entrypoint
CMD ["python", "-m", "src.main"]

# Metadata
LABEL maintainer="eFlow Team"
LABEL description="Reconciliation Service for Data Hub integration"
LABEL version="1.0.0"