-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 5 / Préparation & Qualité — Absences (Streaming Table + Expectations)
-- MAGIC #### Nettoyage et contrôle qualité du flux d'événements d'absence, en réutilisant le même pattern.
-- MAGIC Cohérence des dates, durée plausible, type d'absence connu. On dérive le mois d'absence pour
-- MAGIC les agrégats en aval, et on garde le **motif libre** (`comment`) que le Visual Data Prep
-- MAGIC classera avec l'IA.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_absences
(
  CONSTRAINT valid_absence_id EXPECT (absence_id IS NOT NULL)                       ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_employee   EXPECT (employee_gid IS NOT NULL)                     ON VIOLATION DROP ROW,
  CONSTRAINT valid_dates      EXPECT (start_date IS NOT NULL AND end_date >= start_date) ON VIOLATION DROP ROW,
  CONSTRAINT positive_days    EXPECT (days > 0 AND days <= 365)                     ON VIOLATION DROP ROW,
  CONSTRAINT known_type       EXPECT (absence_type IS NOT NULL)
)
CLUSTER BY AUTO
COMMENT "Événements d'absence nettoyés et typés — couche silver"
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT
  absence_id,
  employee_gid,
  absence_type,
  to_date(start_date)                       AS start_date,
  to_date(end_date)                         AS end_date,
  CAST(days AS INT)                         AS days,
  to_timestamp(event_timestamp)             AS event_timestamp,
  date_trunc('month', to_date(start_date))  AS absence_month,
  NULLIF(trim(comment), '')                 AS comment  -- motif libre (texte), classé par l'IA dans le Visual Data Prep
FROM STREAM(bronze_absences);