# Replication Monitoring

Универсальная система мониторинга и логирования репликации PostgreSQL.

---

## 1. Описание проекта

Система предназначена для автоматической проверки согласованности данных
между продовой базой и её репликой. Решение **универсально** — не привязано
к конкретной базе или таблицам. Всё параметризовано и переиспользуемо.

Основные возможности:
- Получение списка реплицируемых таблиц через `pg_subscription_rel`
- Сравнение количества записей (COUNT) prod vs replica через `dblink`
- Запись результатов в таблицу логов `dba.replication_log`
- Автоматическое создание алармов при превышении порога расхождений
- Поддержка как точного `COUNT(*)`, так и приблизительного подсчёта
  через `pg_class.reltuples` (рекомендуется для таблиц > 40 ГБ)

---

## 2. Требования

- PostgreSQL 10 и выше (логическая репликация)
- Расширение `dblink` (устанавливается автоматически скриптом `001_ddl.sql`)
- Доступ к системным представлениям: `pg_subscription_rel`, `pg_stat_subscription`, `pg_subscription`
- Функции запускаются на стороне **реплики**
- Учётная запись для подключения к prod должна иметь право `SELECT` на проверяемые таблицы

---

## 3. Установка

Выполните SQL-файлы в указанном порядке:

```sql
-- Шаг 1: Создать схему, расширение dblink, таблицы и индексы
\i sql/001_ddl.sql

-- Шаг 2: Создать функции мониторинга
\i sql/002_functions.sql

-- Шаг 3: (опционально) Ознакомиться с примерами вызовов
\i sql/003_examples.sql
```

---

## 4. Структура

### Таблицы

#### `dba.replication_log`
Основная таблица логов. Хранит результаты сравнения COUNT prod vs replica.

| Поле               | Тип            | Описание                              |
|--------------------|----------------|---------------------------------------|
| `id`               | BIGSERIAL PK   | Автоинкремент                         |
| `db_name`          | VARCHAR(128)   | Название базы данных                  |
| `schema_name`      | VARCHAR(128)   | Схема таблицы                         |
| `table_name`       | VARCHAR(128)   | Имя таблицы                           |
| `count_prod`       | BIGINT         | Количество записей на prod            |
| `count_replica`    | BIGINT         | Количество записей на реплике         |
| `count_diff`       | BIGINT         | Расхождение (count_prod - count_replica) |
| `check_date`       | DATE           | Дата проверки (по умолчанию сегодня)  |
| `check_started_at` | TIMESTAMPTZ    | Время начала проверки                 |
| `check_finished_at`| TIMESTAMPTZ    | Время окончания проверки              |

#### `dba.replication_alerts`
Таблица алармов. Создаётся при превышении порога расхождений.

| Поле            | Тип          | Описание                              |
|-----------------|--------------|---------------------------------------|
| `id`            | BIGSERIAL PK | Автоинкремент                         |
| `log_id`        | BIGINT FK    | Ссылка на запись в replication_log    |
| `db_name`       | VARCHAR(128) | Название базы данных                  |
| `schema_name`   | VARCHAR(128) | Схема таблицы                         |
| `table_name`    | VARCHAR(128) | Имя таблицы                           |
| `count_diff`    | BIGINT       | Расхождение                           |
| `threshold`     | BIGINT       | Порог срабатывания                    |
| `alert_message` | TEXT         | Текст сообщения об аларме             |
| `created_at`    | TIMESTAMPTZ  | Время создания алармa                 |

### Функции

| Функция | Назначение |
|---------|-----------|
| `dba.get_replicated_tables()` | Список реплицируемых таблиц из `pg_subscription_rel` |
| `dba.check_replication_counts(connstr, use_approx)` | Сравнение COUNT prod vs replica, запись в лог |
| `dba.check_replication_alerts(threshold, date)` | Поиск расхождений и запись алармов |
| `dba.run_replication_check(connstr, threshold, use_approx)` | Полный цикл: проверка + алармы одной командой |

---

## 5. Использование

### Просмотр реплицируемых таблиц
```sql
SELECT * FROM dba.get_replicated_tables();
```

### Полная проверка с точным COUNT
```sql
SELECT * FROM dba.run_replication_check(
    'dbname=mwallet host=<HOST> port=<PORT> user=dba'
);
```

### Проверка с приблизительным COUNT (для больших таблиц > 40 ГБ)
```sql
SELECT * FROM dba.check_replication_counts(
    'dbname=ewallet host=<HOST> port=<PORT> user=dba',
    TRUE  -- p_use_approximate
);
```

### Проверка алармов за вчера с порогом 100
```sql
SELECT * FROM dba.check_replication_alerts(100, CURRENT_DATE - 1);
```

### Просмотр логов за сегодня
```sql
SELECT * FROM dba.replication_log
WHERE check_date = CURRENT_DATE
ORDER BY ABS(count_diff) DESC;
```

### Таблицы с наибольшими расхождениями
```sql
SELECT db_name, schema_name, table_name, count_diff, check_date
FROM dba.replication_log
WHERE ABS(count_diff) > 0
ORDER BY ABS(count_diff) DESC;
```

---

## 6. Безопасность

**Никогда не храните пароли в SQL-скриптах или коде!**

Используйте файл `~/.pgpass` (на сервере, где работает PostgreSQL-реплика):

```
# Формат: hostname:port:database:username:password
<HOST>:<PORT>:mwallet:dba:ВАШ_ПАРОЛЬ
<HOST>:<PORT>:ewallet:dba:ВАШ_ПАРОЛЬ
```

Установите права доступа:
```bash
chmod 600 ~/.pgpass
```

После этого строка подключения не требует пароля:
```sql
SELECT * FROM dba.run_replication_check(
    'dbname=mwallet host=<HOST> port=<PORT> user=dba'
);
```

---

## 7. Планирование (на будущее)

Пример настройки автоматического запуска через расширение `pg_cron`:

```sql
-- Запуск каждый день в 06:00 для базы mwallet
SELECT cron.schedule(
    'replication-check-mwallet',
    '0 6 * * *',
    $$SELECT * FROM dba.run_replication_check(
        'dbname=mwallet host=<HOST> port=<PORT> user=dba'
    )$$
);

-- Запуск каждый день в 06:05 для базы ewallet
SELECT cron.schedule(
    'replication-check-ewallet',
    '5 6 * * *',
    $$SELECT * FROM dba.run_replication_check(
        'dbname=ewallet host=<HOST> port=<PORT> user=dba'
    )$$
);

-- Просмотр настроенных задач
SELECT * FROM cron.job;

-- Удаление задачи
SELECT cron.unschedule('replication-check-mwallet');
```
