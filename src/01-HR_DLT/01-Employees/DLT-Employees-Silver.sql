-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 2 / Préparation & Qualité — Employés (Streaming Table + Expectations)
-- MAGIC #### Préparation & qualité : typage, colonnes calculées, contrôles qualité (Expectations).
-- MAGIC Les **Expectations** (`CONSTRAINT ... EXPECT`) déclarent les règles qualité directement dans le
-- MAGIC pipeline : lignes non conformes écartées (`DROP ROW`), échec bloquant sur clé manquante
-- MAGIC (`FAIL UPDATE`), ou simple avertissement. On y recrée aussi les **colonnes calculées** du socle
-- MAGIC (ex. `age_bracket`, `seniority_years`) et on **joint le référentiel départements** (master data).

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_employees
(
  CONSTRAINT valid_gid           EXPECT (employee_gid IS NOT NULL)                                 ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_birth_date    EXPECT (birth_date IS NOT NULL AND birth_date > DATE'1940-01-01') ON VIOLATION DROP ROW,
  CONSTRAINT valid_hire_date     EXPECT (hire_date IS NOT NULL AND hire_date >= birth_date)        ON VIOLATION DROP ROW,
  CONSTRAINT plausible_age       EXPECT (age BETWEEN 16 AND 75)                                    ON VIOLATION DROP ROW,
  CONSTRAINT known_department    EXPECT (department_name IS NOT NULL),
  CONSTRAINT valid_contract      EXPECT (contract_type IN ('CDI','CDD','Alternance','Stage','Intérim'))
)
COMMENT "Employés nettoyés, typés, enrichis (age_bracket, ancienneté) et joints au référentiel départements — couche silver"
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT
  b.*,
  d.department_name,
  d.business_unit,
  d.region,
  d.country,
  d.cost_center,
  -- Colonne calculée du socle commun : tranche d'âge dérivée (CASE WHEN, comme une recipe Prepare)
  CASE
    WHEN b.age < 25 THEN '<25'
    WHEN b.age < 35 THEN '25-34'
    WHEN b.age < 45 THEN '35-44'
    WHEN b.age < 55 THEN '45-54'
    ELSE '55+'
  END AS age_bracket,
  CASE
    WHEN b.seniority_years < 2  THEN '0-2 ans'
    WHEN b.seniority_years < 5  THEN '2-5 ans'
    WHEN b.seniority_years < 10 THEN '5-10 ans'
    WHEN b.seniority_years < 20 THEN '10-20 ans'
    ELSE '20+ ans'
  END AS seniority_bracket
FROM (
  SELECT
    employee_gid,
    to_date(extract_date)                                             AS extract_date,
    to_timestamp(extract_ts)                                          AS extract_ts,  -- clé de séquence CDC (monotone)
    initcap(first_name)                                               AS first_name,
    upper(last_name)                                                  AS last_name,
    gender,
    to_date(birth_date)                                              AS birth_date,
    CAST(floor(datediff(current_date(), to_date(birth_date)) / 365.25) AS INT) AS age,
    to_date(hire_date)                                              AS hire_date,
    ROUND(datediff(current_date(), to_date(hire_date)) / 365.25, 1) AS seniority_years,
    job_title,
    contract_type,
    work_location,
    manager_gid,
    lower(email)                                                     AS email,
    CAST(fte AS DOUBLE)                                              AS fte,
    status,
    department_id
  FROM STREAM(bronze_employees)
) b
LEFT JOIN ${master_data_schema}.departments d
  ON b.department_id = d.department_id;