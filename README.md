# Async Property Sync & Scoring Module

> **Тип**: Техническая спецификация (System Architecture & Integration Specification)  
> **Уровень**: Middle System Analyst — Pet-Project  
> **Статус**: In Progress

---

## О проекте

Модуль асинхронной синхронизации и конкурентного скоринга объектов недвижимости между внутренней CRM (50 000+ объектов) и российскими агрегаторами **Авито** и **ЦИАН**.

**Проблема**: синхронный REST-подход не масштабируется при высокочастотных изменениях цен и статусов агентами. **Решение**: event-driven архитектура на базе Apache Kafka с гарантиями At-Least-Once и идемпотентной обработкой на стороне воркеров.

---

## Стек технологий

| Слой | Технология | Назначение |
|------|-----------|------------|
| **Backend** | Python 3.11+, FastAPI, Uvicorn | async-first REST API, валидация Pydantic v2 |
| **Message Broker** | Apache Kafka (acks=all, RF≥3) | At-Least-Once доставка, partition-based ordering |
| **RDBMS** | PostgreSQL 14+ | JSONB, advisory locks, UPSERT с version guard |
| **Cache / Dedup** | Redis Cluster | Распределённая дедупликация, token bucket rate limiting |
| **HTTP Client** | httpx (async) | Connection pooling, exponential backoff |
| **ORM** | SQLAlchemy 2.0 async core | Prepared statements, RETURNING |
| **Monitoring** | Prometheus + Grafana | Per-worker метрики, latency buckets, consumer lag |
| **Logging** | structlog + JSON → ELK | Distributed tracing через correlation_id / trace_id |

---

## Ключевые архитектурные паттерны

### Idempotency (Идемпотентность)
Каждое событие несёт детерминированный `idempotency_key` формата `agent:{id}-prop:{id}-v:{version}-ts:{unix_ms}`. Воркер проверяет ключ в `idempotency_log` (PostgreSQL) перед обработкой — повторная доставка Kafka не приводит к дублю в API агрегатора.

### Version Guard (Защита от гонки данных)
UPSERT в PostgreSQL содержит условие `WHERE property.version < EXCLUDED.version`. Сообщение с устаревшей версией (late arrival) отклоняется атомарно на уровне SQL — без application-level locking. Паттерн реализует Optimistic Concurrency Control (OCC) без явных блокировок на уровне приложения.

### Dead Letter Queue (DLQ)
После 3 неудачных попыток (5xx / timeout) или при первом non-retriable ответе (4xx) событие публикуется в `property.sync.failed.dlq`. Kafka offset коммитится немедленно — partition не блокируется poison pill-ом. Поддерживается ручной replay через Admin API и автоматический reprocessor с TTL-задержкой 1h.

### Two-Level Deduplication
- **L1 — Redis** (`SET NX`, TTL 24h): fast path, in-memory, атомарный
- **L2 — PostgreSQL** (`idempotency_log`): persistent dedup после Redis eviction
- **L3 — Conditional UPSERT**: финальная защита через `ON CONFLICT ... WHERE version < incoming`

---

## Структура репозитория

```
async-property-sync-spec/
├── README.md                        # Этот файл — обзор проекта
└── specs/
    └── part1_architecture.md        # Часть 1: схемы, контракт, edge-cases
```

> **Следующие части** (в разработке):
> - `specs/part2_database_schema.md` — DDL, партиционирование, индексы
> - `specs/part3_scoring_pipeline.md` — windowed SQL, percentile расчёт
> - `specs/part4_deployment_ops.md` — Kafka config, Prometheus alerts, DLQ reprocessor

---

## Быстрая навигация

| Раздел | Файл | Содержание |
|--------|------|------------|
| Архитектурные схемы | [`specs/part1_architecture.md#1`](specs/part1_architecture.md) | Mermaid Sequence Diagrams: генерация события + обработка воркером |
| Message Contract | [`specs/part1_architecture.md#2`](specs/part1_architecture.md) | JSON payload `property.events.v1`, типы полей |
| Edge-Cases | [`specs/part1_architecture.md#3`](specs/part1_architecture.md) | Dedup, Retry/DLQ, Race Conditions |
| Открытые вопросы | [`specs/part1_architecture.md#4`](specs/part1_architecture.md) | Schema Registry, E2E Exactly-Once, Token Rotation |
