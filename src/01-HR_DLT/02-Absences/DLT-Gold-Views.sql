-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 6 / Socle commun — View de consommation (Unity Catalog)
-- MAGIC #### Une **View** persistante (non matérialisée) : la requête s'exécute à l'accès, aucun stockage.
-- MAGIC C'est la vue "socle commun" large exposée aux consommateurs (BI, apps, analystes) : l'état
-- MAGIC courant de chaque employé enrichi de ses KPI d'absence de l'année.
-- MAGIC
-- MAGIC > Les **agrégats de consommation** (effectifs, absentéisme, motifs d'absence classés par l'IA)
-- MAGIC > ne sont plus codés ici : ils sont construits **en no-code** dans un **Visual Data Prep**
-- MAGIC > (Lakeflow Designer), qui lit les tables silver/gold de ce pipeline et publie des
-- MAGIC > Materialized Views. Voir `src/02-Visual-Data-Prep/`.

-- COMMAND ----------

CREATE VIEW gold_hr_common_base
COMMENT "Socle commun HR — vue consolidée employés (état courant) + KPI d'absence de l'année en cours"
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  e.*,
  COALESCE(ab.absence_days_ytd, 0)   AS absence_days_ytd,
  COALESCE(ab.absence_events_ytd, 0) AS absence_events_ytd
FROM gold_employees_current e
LEFT JOIN (
  SELECT
    employee_gid,
    SUM(days)  AS absence_days_ytd,
    COUNT(*)   AS absence_events_ytd
  FROM silver_absences
  WHERE YEAR(start_date) = YEAR(current_date())
  GROUP BY employee_gid
) ab
  ON e.employee_gid = ab.employee_gid
WHERE e.status = 'Active';
