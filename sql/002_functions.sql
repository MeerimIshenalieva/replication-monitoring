-- =============================================================================
-- Файл: 002_functions.sql
-- Описание: Функции универсальной системы мониторинга репликации PostgreSQL.
-- Порядок выполнения: вторым (после 001_ddl.sql)
-- =============================================================================

-- =============================================================================
-- 2.1 Функция: dba.get_replicated_tables()
-- Назначение : Возвращает список всех таблиц, участвующих в логической
--              репликации на данном узле (реплике).
-- Параметры  : нет
-- Возвращает : TABLE (schema_name text, table_name text, sub_name text)
--   schema_name — схема таблицы
--   table_name  — имя таблицы
--   sub_name    — имя подписки (subscription)
-- Примечание  : Функцию следует вызывать на реплике, где настроены подписки.
-- =============================================================================
CREATE OR REPLACE FUNCTION dba.get_replicated_tables()
RETURNS TABLE (
    schema_name TEXT,
    table_name  TEXT,
    sub_name    TEXT
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        -- Извлекаем схему из строки вида "schema.table"
        -- Если схема не указана (нет точки), считаем её 'public'
        CASE
            WHEN position('.' IN srrelid::regclass::text) > 0
            THEN split_part(srrelid::regclass::text, '.', 1)
            ELSE 'public'
        END AS schema_name,

        -- Извлекаем имя таблицы (часть после точки, или всё имя если точки нет)
        CASE
            WHEN position('.' IN srrelid::regclass::text) > 0
            THEN split_part(srrelid::regclass::text, '.', 2)
            ELSE srrelid::regclass::text
        END AS table_name,

        -- Имя подписки
        pss.subname AS sub_name

    FROM pg_subscription_rel psr
    INNER JOIN pg_stat_subscription pss ON pss.subid = psr.srsubid
    INNER JOIN pg_subscription       psb ON psb.oid  = pss.subid

    ORDER BY srrelid::regclass ASC;
$$;

COMMENT ON FUNCTION dba.get_replicated_tables() IS
'Возвращает список всех реплицируемых таблиц на данном узле.
Источник: pg_subscription_rel + pg_stat_subscription + pg_subscription.
Результат: schema_name, table_name, sub_name.';

-- =============================================================================
-- 2.2 Функция: dba.check_replication_counts(p_prod_connstr, p_use_approximate)
-- Назначение : Сравнивает количество записей в таблицах на prod и на реплике.
--              Результаты сохраняются в dba.replication_log.
-- Параметры  :
--   p_prod_connstr    TEXT    — строка подключения к prod через dblink,
--                               например: 'dbname=mydb host=1.2.3.4 port=5432 user=dba'
--                               Пароль рекомендуется хранить в ~/.pgpass.
--   p_use_approximate BOOLEAN — если TRUE, использует приблизительный COUNT
--                               через pg_class.reltuples (быстро для больших таблиц).
--                               По умолчанию FALSE (точный SELECT COUNT(*)).
-- Возвращает : SETOF dba.replication_log — вставленные строки логов
-- Примечание : При ошибке по конкретной таблице пишет RAISE WARNING и
--              продолжает обработку остальных таблиц (count_prod/count_replica = NULL).
-- =============================================================================
CREATE OR REPLACE FUNCTION dba.check_replication_counts(
    p_prod_connstr    TEXT,
    p_use_approximate BOOLEAN DEFAULT FALSE
)
RETURNS SETOF dba.replication_log
LANGUAGE plpgsql
AS $$
DECLARE
    v_table         RECORD;
    v_count_prod    BIGINT;
    v_count_replica BIGINT;
    v_count_diff    BIGINT;
    v_started_at    TIMESTAMPTZ;
    v_finished_at   TIMESTAMPTZ;
    v_db_name       TEXT;
    v_log_row       dba.replication_log;
    -- Динамические SQL-строки для локальных и удалённых запросов
    v_remote_sql    TEXT;
    v_local_sql     TEXT;
BEGIN
    -- Определяем имя базы из строки подключения (для записи в лог)
    -- Ищем 'dbname=<value>' в строке подключения
    v_db_name := trim(
        substring(p_prod_connstr FROM 'dbname\s*=\s*([^\s]+)')
    );
    IF v_db_name IS NULL OR v_db_name = '' THEN
        v_db_name := 'unknown';
    END IF;

    -- Перебираем все реплицируемые таблицы
    FOR v_table IN SELECT * FROM dba.get_replicated_tables()
    LOOP
        v_count_prod    := NULL;
        v_count_replica := NULL;
        v_count_diff    := NULL;

        -- Фиксируем время начала обработки конкретной таблицы
        v_started_at := NOW();

        BEGIN
            IF p_use_approximate THEN
                -- ----------------------------------------------------------------
                -- Приблизительный COUNT через pg_class.reltuples
                -- Рекомендуется для таблиц размером > 40 ГБ
                -- ----------------------------------------------------------------

                -- Запрос на реплике (локально)
                v_local_sql := format(
                    'SELECT reltuples::bigint FROM pg_class '
                    'WHERE relname = %L AND relnamespace = '
                    '(SELECT oid FROM pg_namespace WHERE nspname = %L)',
                    v_table.table_name,
                    v_table.schema_name
                );
                EXECUTE v_local_sql INTO v_count_replica;

                -- Запрос на prod через dblink
                v_remote_sql := format(
                    'SELECT reltuples::bigint FROM pg_class '
                    'WHERE relname = %L AND relnamespace = '
                    '(SELECT oid FROM pg_namespace WHERE nspname = %L)',
                    v_table.table_name,
                    v_table.schema_name
                );
                SELECT r.cnt INTO v_count_prod
                FROM dblink(
                    p_prod_connstr,
                    v_remote_sql
                ) AS r(cnt bigint);

            ELSE
                -- ----------------------------------------------------------------
                -- Точный COUNT(*)
                -- ----------------------------------------------------------------

                -- Запрос на реплике (локально)
                v_local_sql := format(
                    'SELECT COUNT(*) FROM %I.%I',
                    v_table.schema_name,
                    v_table.table_name
                );
                EXECUTE v_local_sql INTO v_count_replica;

                -- Запрос на prod через dblink
                v_remote_sql := format(
                    'SELECT COUNT(*) FROM %I.%I',
                    v_table.schema_name,
                    v_table.table_name
                );
                SELECT r.cnt INTO v_count_prod
                FROM dblink(
                    p_prod_connstr,
                    v_remote_sql
                ) AS r(cnt bigint);
            END IF;

            -- Вычисляем расхождение
            IF v_count_prod IS NOT NULL AND v_count_replica IS NOT NULL THEN
                v_count_diff := v_count_prod - v_count_replica;
            END IF;

        EXCEPTION WHEN OTHERS THEN
            -- При ошибке по конкретной таблице: логируем предупреждение,
            -- оставляем count_prod/count_replica/count_diff = NULL и продолжаем
            RAISE WARNING 'check_replication_counts: ошибка при обработке таблицы %.%: %',
                v_table.schema_name, v_table.table_name, SQLERRM;
        END;

        -- Фиксируем время окончания обработки этой таблицы
        v_finished_at := NOW();

        -- Записываем результат в лог и возвращаем строку
        INSERT INTO dba.replication_log (
            db_name,
            schema_name,
            table_name,
            count_prod,
            count_replica,
            count_diff,
            check_date,
            check_started_at,
            check_finished_at
        )
        VALUES (
            v_db_name,
            v_table.schema_name,
            v_table.table_name,
            v_count_prod,
            v_count_replica,
            v_count_diff,
            CURRENT_DATE,
            v_started_at,
            v_finished_at
        )
        RETURNING * INTO v_log_row;

        RETURN NEXT v_log_row;
    END LOOP;

    RETURN;
END;
$$;

COMMENT ON FUNCTION dba.check_replication_counts(TEXT, BOOLEAN) IS
'Сравнивает количество записей (COUNT) между prod и репликой для всех реплицируемых таблиц.
Параметры:
  p_prod_connstr    — строка подключения к prod через dblink (пароль — из ~/.pgpass).
  p_use_approximate — TRUE для приблизительного COUNT через pg_class.reltuples
                      (рекомендуется для таблиц > 40 ГБ), FALSE для точного COUNT(*).
Результат сохраняется в dba.replication_log и возвращается как SETOF.';

-- =============================================================================
-- 2.3 Функция: dba.check_replication_alerts(p_threshold, p_check_date)
-- Назначение : Анализирует таблицу логов на расхождения сверх порога.
--              Для каждого найденного расхождения создаёт запись в
--              dba.replication_alerts и выводит RAISE WARNING.
-- Параметры  :
--   p_threshold  BIGINT — порог срабатывания (по умолчанию 50 записей)
--   p_check_date DATE   — дата, за которую проверяем (по умолчанию сегодня)
-- Возвращает : SETOF dba.replication_alerts — созданные записи алармов
-- =============================================================================
CREATE OR REPLACE FUNCTION dba.check_replication_alerts(
    p_threshold  BIGINT DEFAULT 50,
    p_check_date DATE   DEFAULT CURRENT_DATE
)
RETURNS SETOF dba.replication_alerts
LANGUAGE plpgsql
AS $$
DECLARE
    v_log         RECORD;
    v_alert_msg   TEXT;
    v_alert_row   dba.replication_alerts;
BEGIN
    -- Ищем записи с расхождением сверх порога за указанную дату
    FOR v_log IN
        SELECT *
        FROM dba.replication_log
        WHERE check_date = p_check_date
          AND ABS(count_diff) > p_threshold
        ORDER BY ABS(count_diff) DESC
    LOOP
        -- Формируем сообщение об аларме
        v_alert_msg := format(
            'ALARM: таблица %s.%s в базе %s — расхождение %s записей (порог: %s)',
            v_log.schema_name,
            v_log.table_name,
            v_log.db_name,
            v_log.count_diff,
            p_threshold
        );

        -- Выводим предупреждение в лог сервера
        RAISE WARNING '%', v_alert_msg;

        -- Вставляем аларм в таблицу и возвращаем строку
        INSERT INTO dba.replication_alerts (
            log_id,
            db_name,
            schema_name,
            table_name,
            count_diff,
            threshold,
            alert_message,
            created_at
        )
        VALUES (
            v_log.id,
            v_log.db_name,
            v_log.schema_name,
            v_log.table_name,
            v_log.count_diff,
            p_threshold,
            v_alert_msg,
            NOW()
        )
        RETURNING * INTO v_alert_row;

        RETURN NEXT v_alert_row;
    END LOOP;

    RETURN;
END;
$$;

COMMENT ON FUNCTION dba.check_replication_alerts(BIGINT, DATE) IS
'Проверяет таблицу dba.replication_log на расхождения сверх допустимого порога.
Параметры:
  p_threshold  — порог по абсолютному значению расхождения (по умолчанию 50).
  p_check_date — дата проверки (по умолчанию CURRENT_DATE).
При превышении порога: записывает аларм в dba.replication_alerts и выполняет RAISE WARNING.';

-- =============================================================================
-- 2.4 Функция: dba.run_replication_check(p_prod_connstr, p_threshold, p_use_approximate)
-- Назначение : Обёртка — выполняет полный цикл проверки репликации:
--              1) dba.check_replication_counts  — сбор и запись логов
--              2) dba.check_replication_alerts  — анализ и запись алармов
--              Удобна для вызова вручную или из планировщика (pg_cron и т.п.).
-- Параметры  :
--   p_prod_connstr    TEXT    — строка подключения к prod (пароль — из ~/.pgpass)
--   p_threshold       BIGINT  — порог алармов (по умолчанию 50)
--   p_use_approximate BOOLEAN — приблизительный COUNT (по умолчанию FALSE)
-- Возвращает : TABLE с полями алармов из dba.replication_alerts
-- =============================================================================
CREATE OR REPLACE FUNCTION dba.run_replication_check(
    p_prod_connstr    TEXT,
    p_threshold       BIGINT  DEFAULT 50,
    p_use_approximate BOOLEAN DEFAULT FALSE
)
RETURNS SETOF dba.replication_alerts
LANGUAGE plpgsql
AS $$
BEGIN
    -- Шаг 1: Собираем данные и записываем в лог
    -- Результаты сохраняются в dba.replication_log
    PERFORM dba.check_replication_counts(p_prod_connstr, p_use_approximate);

    -- Шаг 2: Анализируем лог за сегодня и создаём алармы при превышении порога
    RETURN QUERY
        SELECT * FROM dba.check_replication_alerts(p_threshold, CURRENT_DATE);
END;
$$;

COMMENT ON FUNCTION dba.run_replication_check(TEXT, BIGINT, BOOLEAN) IS
'Обёртка для полного цикла проверки репликации: сбор логов + анализ алармов.
Параметры:
  p_prod_connstr    — строка подключения к prod через dblink (пароль — из ~/.pgpass).
  p_threshold       — порог расхождения для алармов (по умолчанию 50 записей).
  p_use_approximate — TRUE для приблизительного COUNT через pg_class.reltuples
                      (рекомендуется для таблиц > 40 ГБ), FALSE для точного COUNT(*).
Возвращает записи алармов из dba.replication_alerts за текущую дату.';
