-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 6 / Agrégats de consommation — Materialized Views
-- MAGIC #### Datasets de sortie (agrégats) exposés aux analystes / dashboards.
-- MAGIC Une **Materialized View** stocke physiquement le résultat d'une agrégation et se rafraîchit
-- MAGIC (incrémentalement sur serverless). On lit les tables gold/silver en **batch** (pas de `STREAM`)
-- MAGIC car ce sont des agrégats sur l'ensemble des données. On conserve les dimensions clés
-- MAGIC (département, BU, région, mois, type) pour permettre le filtrage en aval.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6a / Effectifs par département (headcount, pyramide des âges, mixité)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_headcount_by_department
COMMENT "Effectifs, âge moyen, ancienneté et mixité par département (état courant)"
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  department_id,
  department_name,
  business_unit,
  region,
  COUNT(*)                                                                    AS headcount,
  ROUND(AVG(age), 1)                                                          AS avg_age,
  ROUND(AVG(seniority_years), 1)                                              AS avg_seniority_years,
  SUM(CASE WHEN contract_type = 'CDI' THEN 1 ELSE 0 END)                      AS cdi_count,
  SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END)                               AS female_count,
  ROUND(100.0 * SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) / COUNT(*), 1)  AS female_pct
FROM gold_employees_current
WHERE status = 'Active'
GROUP BY department_id, department_name, business_unit, region;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6b / Absentéisme par département, mois et type d'absence

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_absenteeism_by_department
COMMENT "Jours et nombre d'absences par département / mois / type d'absence"
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  e.department_id,
  e.department_name,
  e.business_unit,
  a.absence_month,
  a.absence_type,
  COUNT(DISTINCT a.employee_gid)  AS employees_absent,
  COUNT(*)                        AS absence_events,
  SUM(a.days)                     AS total_absence_days,
  ROUND(AVG(a.days), 1)           AS avg_days_per_absence
FROM silver_absences a
INNER JOIN gold_employees_current e
  ON a.employee_gid = e.employee_gid
GROUP BY e.department_id, e.department_name, e.business_unit, a.absence_month, a.absence_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7 / Socle commun — View de consommation (Unity Catalog)
-- MAGIC #### Une **View** persistante (non matérialisée) : la requête s'exécute à l'accès, aucun stockage.
-- MAGIC C'est la vue "socle commun" large exposée aux consommateurs (BI, apps, analystes) : l'état
-- MAGIC courant de chaque employé enrichi de ses KPI d'absence de l'année. Elle illustre la différence
-- MAGIC **View** (calcul à la volée) vs **Materialized View** (résultat stocké et rafraîchi).

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