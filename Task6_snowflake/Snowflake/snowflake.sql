-- 1.
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Создаем базу данных
CREATE DATABASE IF NOT EXISTS AIRLINE_DB;
USE DATABASE AIRLINE_DB;

CREATE SCHEMA IF NOT EXISTS RAW_DATA;
CREATE SCHEMA IF NOT EXISTS SILVER_DATA;
CREATE SCHEMA IF NOT EXISTS GOLD_DATA;
CREATE SCHEMA IF NOT EXISTS AUDIT_DATA;
CREATE SCHEMA IF NOT EXISTS SECURITY;


-- ============================================================
-- 2. СЛОЙ AUDIT (Логирование)
-- ============================================================
CREATE TABLE IF NOT EXISTS AUDIT_DATA.ETL_LOGS (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    procedure_name STRING,
    rows_affected INT,
    status STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- ============================================================
-- 3. СЛОЙ RAW (Сырые данные)
-- ============================================================

CREATE OR REPLACE FILE FORMAT RAW_DATA.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;

-- 3.1. Создаем таблицу
CREATE TABLE IF NOT EXISTS RAW_DATA.FLIGHTS_RAW (
    row_index STRING,
    passenger_id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    age STRING,
    nationality STRING,
    airport_name STRING,
    airport_country_code STRING,
    country_name STRING,
    airport_continent STRING,
    continents STRING,
    departure_date STRING,
    arrival_airport STRING,
    pilot_name STRING,
    flight_status STRING,
    ticket_type STRING,
    passenger_status STRING
);

-- 3.2. Создаем СТРИМ на таблице RAW
CREATE OR REPLACE STREAM RAW_DATA.FLIGHTS_STREAM_ON_RAW
ON TABLE RAW_DATA.FLIGHTS_RAW;

-- !!! МАГИЧЕСКИЙ БЛОК: ПЕРЕЗАПУСК ДАННЫХ !!!
-- Чтобы стрим увидел уже загруженные данные
BEGIN
    IF ((SELECT COUNT(*) FROM RAW_DATA.FLIGHTS_RAW) > 0) THEN
        CREATE OR REPLACE TEMPORARY TABLE RAW_DATA.TEMP_FLIGHTS AS SELECT * FROM RAW_DATA.FLIGHTS_RAW;
        TRUNCATE TABLE RAW_DATA.FLIGHTS_RAW;
        INSERT INTO RAW_DATA.FLIGHTS_RAW SELECT * FROM RAW_DATA.TEMP_FLIGHTS;
    END IF;
END;


-- ============================================================
-- 4. СЛОЙ SILVER (Очистка и дедупликация)
-- ============================================================

CREATE TABLE IF NOT EXISTS SILVER_DATA.FLIGHTS_SILVER (
    flight_hash STRING PRIMARY KEY,
    passenger_id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    age NUMBER,
    nationality STRING,
    airport_name STRING,
    country_name STRING,
    departure_date DATE,
    arrival_airport STRING,
    pilot_name STRING,
    flight_status STRING,
    ticket_type STRING,
    passenger_status STRING,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM SILVER_DATA.FLIGHTS_SILVER_STREAM
ON TABLE SILVER_DATA.FLIGHTS_SILVER;

CREATE OR REPLACE PROCEDURE SILVER_DATA.PROCESS_FLIGHTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted INT;
BEGIN
    MERGE INTO AIRLINE_DB.SILVER_DATA.FLIGHTS_SILVER AS TARGET
    USING (
        SELECT
            MD5(CONCAT(row_index, passenger_id, departure_date, arrival_airport)) as flight_hash,
            passenger_id, first_name, last_name, gender,
            TRY_TO_NUMBER(age) as age,
            nationality, airport_name, country_name,
            TRY_TO_DATE(departure_date, 'MM/DD/YYYY') as departure_date,
            arrival_airport, pilot_name, flight_status, ticket_type, passenger_status
        FROM AIRLINE_DB.RAW_DATA.FLIGHTS_STREAM_ON_RAW
        WHERE METADATA$ACTION = 'INSERT'
    ) AS SOURCE
    ON TARGET.flight_hash = SOURCE.flight_hash
    WHEN NOT MATCHED THEN INSERT (
        flight_hash, passenger_id, first_name, last_name, gender, age,
        nationality, airport_name, country_name, departure_date,
        arrival_airport, pilot_name, flight_status, ticket_type, passenger_status
    ) VALUES (
        SOURCE.flight_hash, SOURCE.passenger_id, SOURCE.first_name, SOURCE.last_name,
        SOURCE.gender, SOURCE.age, SOURCE.nationality, SOURCE.airport_name,
        SOURCE.country_name, SOURCE.departure_date, SOURCE.arrival_airport,
        SOURCE.pilot_name, SOURCE.flight_status, SOURCE.ticket_type, SOURCE.passenger_status
    );

    rows_inserted := SQLROWCOUNT;

    INSERT INTO AIRLINE_DB.AUDIT_DATA.ETL_LOGS (procedure_name, rows_affected, status)
    VALUES ('PROCESS_FLIGHTS_SILVER', :rows_inserted, 'SUCCESS');

    COMMIT;
    RETURN 'Processed ' || :rows_inserted || ' rows into Silver.';
END;
$$;


-- ============================================================
-- 5. СЛОЙ GOLD (Агрегация и отчеты)
-- ============================================================

CREATE TABLE IF NOT EXISTS GOLD_DATA.DAILY_AIRPORT_STATS (
    stat_date DATE,
    airport_name STRING,
    total_flights INT,
    unique_pilots INT,
    passengers_served INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE GOLD_DATA.PROCESS_DAILY_STATS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_affected INT;
BEGIN
    MERGE INTO AIRLINE_DB.GOLD_DATA.DAILY_AIRPORT_STATS AS TARGET
    USING (
        SELECT
            departure_date,
            airport_name,
            COUNT(flight_hash) as flight_count,
            COUNT(DISTINCT pilot_name) as pilot_count,
            COUNT(passenger_id) as passenger_count
        FROM AIRLINE_DB.SILVER_DATA.FLIGHTS_SILVER_STREAM
        WHERE METADATA$ACTION = 'INSERT'
        GROUP BY departure_date, airport_name
    ) AS SOURCE
    ON TARGET.stat_date = SOURCE.departure_date
       AND TARGET.airport_name = SOURCE.airport_name
    WHEN MATCHED THEN UPDATE SET
        TARGET.total_flights = TARGET.total_flights + SOURCE.flight_count,
        TARGET.unique_pilots = GREATEST(TARGET.unique_pilots, SOURCE.pilot_count),
        TARGET.passengers_served = TARGET.passengers_served + SOURCE.passenger_count,
        TARGET.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        stat_date, airport_name, total_flights, unique_pilots, passengers_served
    ) VALUES (
        SOURCE.departure_date, SOURCE.airport_name, SOURCE.flight_count, SOURCE.pilot_count, SOURCE.passenger_count
    );

    rows_affected := SQLROWCOUNT;

    INSERT INTO AIRLINE_DB.AUDIT_DATA.ETL_LOGS (procedure_name, rows_affected, status)
    VALUES ('PROCESS_DAILY_STATS_GOLD', :rows_affected, 'SUCCESS');

    COMMIT;
    RETURN 'Gold Layer Updated. Rows merged: ' || :rows_affected;
END;
$$;


-- ============================================================
-- 6. БЕЗОПАСНОСТЬ (Исправлено!)
-- ============================================================

CREATE OR REPLACE TABLE SECURITY.ACCESS_MAPPING (
    role_name STRING,
    allowed_country STRING
);

INSERT INTO SECURITY.ACCESS_MAPPING (role_name, allowed_country)
VALUES ('ACCOUNTADMIN', 'ALL'), ('FRANCE_MANAGER', 'France');

-- !!! ИСПРАВЛЕНИЕ: БЕЗОПАСНОЕ СНЯТИЕ ПОЛИТИКИ !!!
-- Мы пытаемся снять политику с таблицы перед её обновлением.
-- Оборачиваем в BEGIN/EXCEPTION, чтобы не падало, если политики там еще нет.
BEGIN
   ALTER TABLE SILVER_DATA.FLIGHTS_SILVER DROP ROW ACCESS POLICY SECURITY.COUNTRY_ACCESS_POLICY;
EXCEPTION
   WHEN OTHER THEN
      RETURN 'Policy was not attached, continuing...';
END;

-- Теперь спокойно создаем или обновляем политику
CREATE OR REPLACE ROW ACCESS POLICY SECURITY.COUNTRY_ACCESS_POLICY
AS (country_val STRING) RETURNS BOOLEAN ->
    EXISTS (SELECT 1 FROM SECURITY.ACCESS_MAPPING WHERE role_name = CURRENT_ROLE() AND allowed_country = 'ALL')
    OR
    EXISTS (SELECT 1 FROM SECURITY.ACCESS_MAPPING WHERE role_name = CURRENT_ROLE() AND allowed_country = country_val);

-- И привязываем обратно
ALTER TABLE SILVER_DATA.FLIGHTS_SILVER
ADD ROW ACCESS POLICY SECURITY.COUNTRY_ACCESS_POLICY ON (country_name);

CREATE OR REPLACE SECURE VIEW GOLD_DATA.FLIGHTS_SECURE_REPORT AS
SELECT
    passenger_id, first_name, last_name, country_name, flight_status
FROM SILVER_DATA.FLIGHTS_SILVER;

GRANT ALL ON DATABASE AIRLINE_DB TO ROLE ACCOUNTADMIN;


-- ============================================================
-- 7. ЗАПУСК ПАЙПЛАЙНА
-- ============================================================

CALL SILVER_DATA.PROCESS_FLIGHTS();
CALL GOLD_DATA.PROCESS_DAILY_STATS();

SELECT * FROM GOLD_DATA.DAILY_AIRPORT_STATS;