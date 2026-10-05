-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Exploration & validation — HR 360
-- MAGIC Requêtes prêtes à l'emploi pour la démo/enablement, à lancer sur un SQL Warehouse après
-- MAGIC exécution du pipeline. Adapter `alp_demo_catalog.acme_hr` à votre catalog / schéma si besoin.

-- COMMAND ----------

-- MAGIC %md ## Streaming tables (bronze / silver)

-- COMMAND ----------

SELECT count(*) AS bronze_rows FROM alp_demo_catalog.acme_hr.bronze_employees;
SELECT * FROM alp_demo_catalog.acme_hr.silver_employees LIMIT 20;

-- COMMAND ----------

-- MAGIC %md ## Concaténation des 2 filiales (Append Flows)
-- MAGIC Une seule table bronze, alimentée par un flow par Volume : `source_entity` indique l'origine.

-- COMMAND ----------

SELECT source_entity, count(*) AS lignes, count(DISTINCT _source_file) AS fichiers,
       count(DISTINCT employee_id) AS employes
FROM alp_demo_catalog.acme_hr.bronze_employees
GROUP BY source_entity ORDER BY source_entity;

SELECT source_entity, count(*) AS absences
FROM alp_demo_catalog.acme_hr.bronze_absences
GROUP BY source_entity ORDER BY source_entity;

-- COMMAND ----------

-- MAGIC %md ## Dernière livraison dédupliquée (window functions)
-- MAGIC `silver_employees_last_extract` : une ligne par employé dans le dernier fichier de chaque filiale.

-- COMMAND ----------

-- Doublons intra-fichier : combien d'employés avaient plusieurs versions dans le dernier fichier ?
SELECT source_entity, versions_in_file, count(*) AS employes
FROM alp_demo_catalog.acme_hr.silver_employees_last_extract
GROUP BY source_entity, versions_in_file
ORDER BY source_entity, versions_in_file;

-- Ce que les corrections intra-journée ont changé (LAG)
SELECT source_entity, employee_id, versions_in_file,
       previous_department_id, department_id, previous_job_title, job_title, updated_at
FROM alp_demo_catalog.acme_hr.silver_employees_last_extract
WHERE versions_in_file > 1
ORDER BY employee_id
LIMIT 20;

-- COMMAND ----------

-- Avant / après pour un employé dupliqué : toutes ses versions brutes, puis la ligne retenue
WITH one_emp AS (
  SELECT employee_id FROM alp_demo_catalog.acme_hr.silver_employees_last_extract
  WHERE versions_in_file > 1 ORDER BY employee_id LIMIT 1
)
SELECT 'silver (toutes les versions)' AS source, s.employee_id, s.updated_at, s.department_id, s.job_title
FROM alp_demo_catalog.acme_hr.silver_employees s
JOIN alp_demo_catalog.acme_hr.silver_employees_last_extract l
  ON s.employee_id = l.employee_id AND s._source_file = l._source_file
WHERE s.employee_id IN (SELECT employee_id FROM one_emp)
UNION ALL
SELECT 'last_extract (rang 1)', employee_id, updated_at, department_id, job_title
FROM alp_demo_catalog.acme_hr.silver_employees_last_extract
WHERE employee_id IN (SELECT employee_id FROM one_emp)
ORDER BY source, updated_at;

-- COMMAND ----------

-- Cohérence : pour les employés du dernier fichier, Auto CDC (gold) et la window function
-- doivent retenir la même version. Attendu : 0 écart.
SELECT count(*) AS ecarts
FROM alp_demo_catalog.acme_hr.silver_employees_last_extract l
JOIN alp_demo_catalog.acme_hr.gold_employees_current g ON l.employee_id = g.employee_id
WHERE l.department_id <> g.department_id OR l.job_title <> g.job_title
   OR l.updated_at <> g.updated_at;

-- COMMAND ----------

-- MAGIC %md ## Gold — état courant (SCD Type 1) : une ligne par employé

-- COMMAND ----------

SELECT count(*) AS employes_courants FROM alp_demo_catalog.acme_hr.gold_employees_current;

SELECT age_bracket, count(*) AS effectif
FROM alp_demo_catalog.acme_hr.gold_employees_current
WHERE status = 'Active'
GROUP BY age_bracket ORDER BY age_bracket;

-- COMMAND ----------

-- MAGIC %md ## Gold — historique (SCD Type 2) : mobilité dans le temps
-- MAGIC L'état courant = `__END_AT IS NULL`. Les lignes clôturées sont l'historique.

-- COMMAND ----------


-- Employés ayant connu au moins un changement historisé
SELECT employee_id FROM alp_demo_catalog.acme_hr.gold_employees_history
GROUP BY employee_id HAVING count(*) > 1;

-- COMMAND ----------

SELECT employee_id, department_id, contract_type, job_title, __START_AT, __END_AT
FROM alp_demo_catalog.acme_hr.gold_employees_history
WHERE employee_id = 'EMPFR0000010'  -- remplacer par un employee_id renvoyé par la requête ci-dessus
ORDER BY __START_AT;


-- COMMAND ----------

-- MAGIC %md ## Materialized views (construites en no-code dans le Visual Data Prep)
-- MAGIC À lancer après le job `hr_360_job` (pipeline SDP → Visual Data Prep).

-- COMMAND ----------

SELECT * FROM alp_demo_catalog.acme_hr.gold_headcount_by_department ORDER BY headcount DESC;

SELECT business_unit, absence_type, sum(total_absence_days) AS jours_absence
FROM alp_demo_catalog.acme_hr.gold_absenteeism_by_department
GROUP BY business_unit, absence_type
ORDER BY jours_absence DESC;

-- COMMAND ----------

-- MAGIC %md ## Motifs d'absence classés par l'IA (AI Function `ai_classify`)

-- COMMAND ----------

SELECT reason_category, sum(absence_events) AS absences, sum(total_absence_days) AS jours
FROM alp_demo_catalog.acme_hr.gold_absence_reasons_by_department
GROUP BY reason_category
ORDER BY jours DESC;

-- Absences « liées au travail » par département : un signal santé & sécurité pour les RH
SELECT department_name, absence_events, total_absence_days
FROM alp_demo_catalog.acme_hr.gold_absence_reasons_by_department
WHERE reason_category = 'lié au travail'
ORDER BY total_absence_days DESC;

-- COMMAND ----------

-- MAGIC %md ## Liquid clustering automatique (`CLUSTER BY AUTO`)

-- COMMAND ----------

DESCRIBE DETAIL alp_demo_catalog.acme_hr.silver_absences;

-- COMMAND ----------

-- MAGIC %md ## Materialized View — vue 360° employé (consommation)

-- COMMAND ----------

SELECT source_entity, employee_id, last_name, department_name, business_unit, age_bracket,
       seniority_years, absence_days_ytd
FROM alp_demo_catalog.acme_hr.gold_employee_360
ORDER BY absence_days_ytd DESC
LIMIT 20;