-- =============================================================================
-- Файл: 001_ddl.sql
-- Описание: DDL для универсальной системы мониторинга репликации PostgreSQL.
--           Создаёт схему dba, подключает расширение dblink,
--           таблицы логов и алармов с необходимыми индексами.
-- Порядок выполнения: первым (до 002_functions.sql)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1.1 Схема и расширение
-- -----------------------------------------------------------------------------

-- Создаём схему dba для объектов мониторинга
CREATE SCHEMA IF NOT EXISTS dba;

-- Подключаем расширение dblink для удалённых запросов к prod-базе
CREATE EXTENSION IF NOT EXISTS dblink;

-- -----------------------------------------------------------------------------
-- 1.2 Таблица логов: dba.replication_log
-- Хранит результаты сравнения количества записей prod vs replica
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dba.replication_log (
    -- Суррогатный первичный ключ (автоинкремент)
    id                BIGSERIAL       PRIMARY KEY,

    -- Название базы данных, для которой выполнялась проверка
    db_name           VARCHAR(128)    NOT NULL,

    -- Название схемы таблицы
    schema_name       VARCHAR(128)    NOT NULL,

    -- Название таблицы
    table_name        VARCHAR(128)    NOT NULL,

    -- Количество записей на prod (может быть NULL при ошибке получения)
    count_prod        BIGINT,

    -- Количество записей на реплике (может быть NULL при ошибке получения)
    count_replica     BIGINT,

    -- Расхождение: count_prod - count_replica (может быть NULL при ошибке)
    count_diff        BIGINT,

    -- Дата проверки (по умолчанию — текущая дата)
    check_date        DATE            NOT NULL DEFAULT CURRENT_DATE,

    -- Дата и время начала проверки
    check_started_at  TIMESTAMPTZ,

    -- Дата и время окончания проверки
    check_finished_at TIMESTAMPTZ
);

-- Индекс по дате проверки — ускоряет выборку логов за конкретный день
CREATE INDEX IF NOT EXISTS idx_replication_log_check_date
    ON dba.replication_log (check_date);

-- Составной индекс по базе/схеме/таблице — ускоряет поиск по конкретному объекту
CREATE INDEX IF NOT EXISTS idx_replication_log_db_schema_table
    ON dba.replication_log (db_name, schema_name, table_name);

-- Индекс по расхождению — ускоряет поиск проблемных записей
CREATE INDEX IF NOT EXISTS idx_replication_log_count_diff
    ON dba.replication_log (count_diff);

-- -----------------------------------------------------------------------------
-- 1.3 Таблица алармов: dba.replication_alerts
-- Хранит записи о превышении допустимого порога расхождений
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dba.replication_alerts (
    -- Суррогатный первичный ключ
    id             BIGSERIAL    PRIMARY KEY,

    -- Ссылка на запись лога, породившую аларм
    log_id         BIGINT       REFERENCES dba.replication_log (id),

    -- Название базы данных
    db_name        VARCHAR(128),

    -- Название схемы таблицы
    schema_name    VARCHAR(128),

    -- Название таблицы
    table_name     VARCHAR(128),

    -- Фактическое расхождение в записях
    count_diff     BIGINT,

    -- Порог срабатывания, при котором был создан аларм
    threshold      BIGINT,

    -- Текстовое сообщение об аларме
    alert_message  TEXT,

    -- Дата и время создания алармa
    created_at     TIMESTAMPTZ  DEFAULT NOW()
);
