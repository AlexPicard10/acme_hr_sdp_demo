-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Exploration & validation — HR 360
-- MAGIC Requêtes prêtes à l'emploi pour la démo/enablement, à lancer sur un SQL Warehouse après
-- MAGIC exécution des pipelines. Les tables du pipeline SQL sont dans `alp_demo_catalog.acme_hr_sql`,
-- MAGIC celles du pipeline Python dans `alp_demo_catalog.acme_hr_python` (mêmes noms). Adapter le
-- MAGIC catalog / schéma si besoin.

-- COMMAND ----------

-- MAGIC %md ## Streaming tables (bronze / silver)

-- COMMAND ----------

SELECT count(*) AS bronze_rows FROM alp_demo_catalog.acme_hr_sql.bronze_employees;
SELECT * FROM alp_demo_catalog.acme_hr_sql.silver_employees LIMIT 20;

-- COMMAND ----------

-- MAGIC %md ## Concaténation des 2 filiales (Append Flows)
-- MAGIC Une seule table bronze, alimentée par un flow par Volume : `source_entity` indique l'origine.

-- COMMAND ----------

SELECT source_entity, count(*) AS lignes, count(DISTINCT _source_file) AS fichiers,
       count(DISTINCT employee_id) AS employes
FROM alp_demo_catalog.acme_hr_sql.bronze_employees
GROUP BY source_entity ORDER BY source_entity;

SELECT source_entity, count(*) AS absences
FROM alp_demo_catalog.acme_hr_sql.bronze_absences
GROUP BY source_entity ORDER BY source_entity;

-- COMMAND ----------

-- MAGIC %md ## Dernière livraison dédupliquée (window functions)
-- MAGIC `silver_employees_last_extract` : une ligne par employé dans le dernier fichier de chaque filiale.

-- COMMAND ----------

-- Doublons intra-fichier : combien d'employés avaient plusieurs versions dans le dernier fichier ?
SELECT source_entity, versions_in_file, count(*) AS employes
FROM alp_demo_catalog.acme_hr_sql.silver_employees_last_extract
GROUP BY source_entity, versions_in_file
ORDER BY source_entity, versions_in_file;

-- Ce que les corrections intra-journée ont changé (LAG)
SELECT source_entity, employee_id, versions_in_file,
       previous_department_id, department_id, previous_job_title, job_title, updated_at
FROM alp_demo_catalog.acme_hr_sql.silver_employees_last_extract
WHERE versions_in_file > 1
ORDER BY employee_id
LIMIT 20;

-- COMMAND ----------

-- Avant / après pour un employé dupliqué : toutes ses versions brutes, puis la ligne retenue
WITH one_emp AS (
  SELECT employee_id, _source_file
  FROM alp_demo_catalog.acme_hr_sql.silver_employees_last_extract
  WHERE versions_in_file > 1
  ORDER BY employee_id
  LIMIT 1
),
all_versions AS (
  SELECT 'silver (toutes les versions)' AS source, s.employee_id, s.updated_at, s.department_id, s.job_title
  FROM alp_demo_catalog.acme_hr_sql.silver_employees s
  JOIN one_emp o ON s.employee_id = o.employee_id AND s._source_file = o._source_file
),
kept_version AS (
  SELECT 'last_extract (rang 1)' AS source, l.employee_id, l.updated_at, l.department_id, l.job_title
  FROM alp_demo_catalog.acme_hr_sql.silver_employees_last_extract l
  JOIN one_emp o ON l.employee_id = o.employee_id
)
SELECT * FROM all_versions
UNION ALL
SELECT * FROM kept_version
ORDER BY source, updated_at;

-- COMMAND ----------

-- Cohérence : pour les employés du dernier fichier, Auto CDC (gold) et la window function
-- doivent retenir la même version. Attendu : 0 écart.
SELECT count(*) AS ecarts
FROM alp_demo_catalog.acme_hr_sql.silver_employees_last_extract l
JOIN alp_demo_catalog.acme_hr_sql.gold_employees_current g ON l.employee_id = g.employee_id
WHERE l.department_id <> g.department_id OR l.job_title <> g.job_title
   OR l.updated_at <> g.updated_at;

-- COMMAND ----------

-- MAGIC %md ## Gold — état courant (SCD Type 1) : une ligne par employé

-- COMMAND ----------

SELECT count(*) AS employes_courants FROM alp_demo_catalog.acme_hr_sql.gold_employees_current;

SELECT age_bracket, count(*) AS effectif
FROM alp_demo_catalog.acme_hr_sql.gold_employees_current
WHERE status = 'Active'
GROUP BY age_bracket ORDER BY age_bracket;

-- COMMAND ----------

-- MAGIC %md ## Gold — historique (SCD Type 2) : mobilité dans le temps
-- MAGIC L'état courant = `__END_AT IS NULL`. Les lignes clôturées sont l'historique.

-- COMMAND ----------

-- Employés ayant connu au moins un changement historisé
SELECT employee_id FROM alp_demo_catalog.acme_hr_sql.gold_employees_history
GROUP BY employee_id HAVING count(*) > 1;

-- COMMAND ----------

SELECT employee_id, department_id, contract_type, job_title, __START_AT, __END_AT
FROM alp_demo_catalog.acme_hr_sql.gold_employees_history
WHERE employee_id = 'EMPFR0000010'  -- remplacer par un employee_id renvoyé par la requête ci-dessus
ORDER BY __START_AT;

-- COMMAND ----------

-- MAGIC %md ## SQL vs Python : mêmes résultats ?
-- MAGIC Les deux pipelines lisent les **mêmes Volumes** et appliquent la même logique : leurs tables
-- MAGIC (mêmes noms, deux schémas) doivent contenir les mêmes données. Les tables silver/gold ne portent
-- MAGIC pas l'horodatage d'ingestion (`_ingested_at`, propre à chaque run) : on peut les comparer telles
-- MAGIC quelles. `EXCEPT` compare les colonnes **par position** : les deux pipelines produisent les
-- MAGIC colonnes dans le même ordre.

-- COMMAND ----------

-- Comptages côte à côte
WITH sql_counts AS (
  SELECT 'bronze_employees' AS table_name, count(*) AS n FROM alp_demo_catalog.acme_hr_sql.bronze_employees
  UNION ALL SELECT 'bronze_absences',               count(*) FROM alp_demo_catalog.acme_hr_sql.bronze_absences
  UNION ALL SELECT 'silver_employees',              count(*) FROM alp_demo_catalog.acme_hr_sql.silver_employees
  UNION ALL SELECT 'silver_employees_last_extract', count(*) FROM alp_demo_catalog.acme_hr_sql.silver_employees_last_extract
  UNION ALL SELECT 'silver_absences',               count(*) FROM alp_demo_catalog.acme_hr_sql.silver_absences
  UNION ALL SELECT 'gold_employees_current',        count(*) FROM alp_demo_catalog.acme_hr_sql.gold_employees_current
  UNION ALL SELECT 'gold_employees_history',        count(*) FROM alp_demo_catalog.acme_hr_sql.gold_employees_history
  UNION ALL SELECT 'gold_employee_360',             count(*) FROM alp_demo_catalog.acme_hr_sql.gold_employee_360
),
python_counts AS (
  SELECT 'bronze_employees' AS table_name, count(*) AS n FROM alp_demo_catalog.acme_hr_python.bronze_employees
  UNION ALL SELECT 'bronze_absences',               count(*) FROM alp_demo_catalog.acme_hr_python.bronze_absences
  UNION ALL SELECT 'silver_employees',              count(*) FROM alp_demo_catalog.acme_hr_python.silver_employees
  UNION ALL SELECT 'silver_employees_last_extract', count(*) FROM alp_demo_catalog.acme_hr_python.silver_employees_last_extract
  UNION ALL SELECT 'silver_absences',               count(*) FROM alp_demo_catalog.acme_hr_python.silver_absences
  UNION ALL SELECT 'gold_employees_current',        count(*) FROM alp_demo_catalog.acme_hr_python.gold_employees_current
  UNION ALL SELECT 'gold_employees_history',        count(*) FROM alp_demo_catalog.acme_hr_python.gold_employees_history
  UNION ALL SELECT 'gold_employee_360',             count(*) FROM alp_demo_catalog.acme_hr_python.gold_employee_360
)
SELECT s.table_name, s.n AS rows_sql, p.n AS rows_python, s.n = p.n AS identique
FROM sql_counts s
JOIN python_counts p USING (table_name)
ORDER BY s.table_name;

-- COMMAND ----------

-- Contenu identique, dans les deux sens (attendu : 0 ligne pour chaque requête)
WITH sql_current AS (
  SELECT * FROM alp_demo_catalog.acme_hr_sql.gold_employees_current
),
python_current AS (
  SELECT * FROM alp_demo_catalog.acme_hr_python.gold_employees_current
),
sql_only AS (
  SELECT * FROM sql_current EXCEPT SELECT * FROM python_current
),
python_only AS (
  SELECT * FROM python_current EXCEPT SELECT * FROM sql_current
)
SELECT 'sql sans python' AS ecart, count(*) AS n FROM sql_only
UNION ALL
SELECT 'python sans sql', count(*) FROM python_only;

WITH sql_last AS (
  SELECT * FROM alp_demo_catalog.acme_hr_sql.silver_employees_last_extract
),
python_last AS (
  SELECT * FROM alp_demo_catalog.acme_hr_python.silver_employees_last_extract
),
sql_only AS (
  SELECT * FROM sql_last EXCEPT SELECT * FROM python_last
),
python_only AS (
  SELECT * FROM python_last EXCEPT SELECT * FROM sql_last
)
SELECT 'sql sans python' AS ecart, count(*) AS n FROM sql_only
UNION ALL
SELECT 'python sans sql', count(*) FROM python_only;

-- COMMAND ----------

-- MAGIC %md ## Materialized views (construites en no-code dans le Visual Data Prep)
-- MAGIC À lancer après le Visual Data Prep (il lit et écrit dans `acme_hr_sql`).

-- COMMAND ----------

SELECT * FROM alp_demo_catalog.acme_hr_sql.gold_headcount_by_department ORDER BY headcount DESC;

SELECT business_unit, absence_type, sum(total_absence_days) AS jours_absence
FROM alp_demo_catalog.acme_hr_sql.gold_absenteeism_by_department
GROUP BY business_unit, absence_type
ORDER BY jours_absence DESC;

-- COMMAND ----------

-- MAGIC %md ## Motifs d'absence classés par l'IA (AI Function `ai_classify`)

-- COMMAND ----------

SELECT reason_category, sum(absence_events) AS absences, sum(total_absence_days) AS jours
FROM alp_demo_catalog.acme_hr_sql.gold_absence_reasons_by_department
GROUP BY reason_category
ORDER BY jours DESC;

-- Absences « liées au travail » par département : un signal santé & sécurité pour les RH
SELECT department_name, absence_events, total_absence_days
FROM alp_demo_catalog.acme_hr_sql.gold_absence_reasons_by_department
WHERE reason_category = 'lié au travail'
ORDER BY total_absence_days DESC;

-- COMMAND ----------

-- MAGIC %md ## Liquid clustering automatique (`CLUSTER BY AUTO`)

-- COMMAND ----------

DESCRIBE DETAIL alp_demo_catalog.acme_hr_sql.silver_absences;

-- COMMAND ----------

-- MAGIC %md ## Materialized View — vue 360° employé (consommation)

-- COMMAND ----------

SELECT source_entity, employee_id, last_name, department_name, business_unit, age_bracket,
       seniority_years, absence_days_ytd
FROM alp_demo_catalog.acme_hr_sql.gold_employee_360
ORDER BY absence_days_ytd DESC
LIMIT 20;
