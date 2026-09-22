-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Exploration & validation — HR Common Base
-- MAGIC Requêtes prêtes à l'emploi pour la démo/enablement, à lancer sur un SQL Warehouse après
-- MAGIC exécution du pipeline. Remplacer `alp_demo_catalog.dev` par le schéma cible si besoin.

-- COMMAND ----------

-- MAGIC %md ## Streaming tables (bronze / silver)

-- COMMAND ----------

SELECT count(*) AS bronze_rows FROM alp_demo_catalog.acme_hr.bronze_employees;
SELECT * FROM alp_demo_catalog.acme_hr.silver_employees LIMIT 20;

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
SELECT employee_gid FROM alp_demo_catalog.acme_hr.gold_employees_history
GROUP BY employee_gid HAVING count(*) > 1

-- COMMAND ----------

SELECT employee_gid, department_id, contract_type, job_title, __START_AT, __END_AT
FROM alp_demo_catalog.acme_hr.gold_employees_history
WHERE employee_gid = 'GID0041944'
ORDER BY __START_AT;


-- COMMAND ----------

-- MAGIC %md ## Materialized views

-- COMMAND ----------

SELECT * FROM alp_demo_catalog.acme_hr.gold_headcount_by_department ORDER BY headcount DESC;

SELECT business_unit, absence_type, sum(total_absence_days) AS jours_absence
FROM alp_demo_catalog.acme_hr.gold_absenteeism_by_department
GROUP BY business_unit, absence_type
ORDER BY jours_absence DESC;

-- COMMAND ----------

-- MAGIC %md ## View — socle commun (consommation)

-- COMMAND ----------

SELECT employee_gid, last_name, department_name, business_unit, age_bracket,
       seniority_years, absence_days_ytd
FROM alp_demo_catalog.acme_hr.gold_hr_common_base
ORDER BY absence_days_ytd DESC
LIMIT 20;