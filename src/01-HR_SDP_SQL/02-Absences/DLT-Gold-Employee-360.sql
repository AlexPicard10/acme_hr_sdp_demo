-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 6 / Vue 360° employé — Materialized View de consommation (Unity Catalog)
-- MAGIC #### Une **Materialized View** : le résultat est stocké et rafraîchi à chaque update du pipeline.
-- MAGIC C'est la table « vue 360° » large exposée aux consommateurs (BI, apps, analystes) : l'état
-- MAGIC courant de chaque employé enrichi de ses KPI d'absence de l'année. On la matérialise car elle
-- MAGIC est lue souvent et coûte une jointure + une agrégation : les lecteurs obtiennent un résultat
-- MAGIC déjà calculé (et clusterisé automatiquement) au lieu de relancer la requête à chaque accès.
-- MAGIC
-- MAGIC > Les **agrégats de consommation** (effectifs, absentéisme, motifs d'absence classés par l'IA)
-- MAGIC > ne sont plus codés ici : ils sont construits **en no-code** dans un **Visual Data Prep**
-- MAGIC > (Lakeflow Designer), qui lit les tables silver/gold de ce pipeline et publie des
-- MAGIC > Materialized Views. Voir `src/02-Visual-Data-Prep/`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_employee_360
CLUSTER BY AUTO
COMMENT "Vue 360° employé — consolidation (état courant) + KPI d'absence de l'année en cours"
TBLPROPERTIES ('quality' = 'gold')
AS
-- Étape 1 (CTE) : KPI d'absence de l'année en cours, par employé
WITH absences_ytd AS (
  SELECT
    employee_id,
    SUM(days)  AS absence_days_ytd,
    COUNT(*)   AS absence_events_ytd
  FROM silver_absences
  WHERE YEAR(start_date) = YEAR(current_date())
  GROUP BY employee_id
)
-- Étape 2 : état courant de chaque employé actif, enrichi des KPI
SELECT
  e.*,
  COALESCE(ab.absence_days_ytd, 0)   AS absence_days_ytd,
  COALESCE(ab.absence_events_ytd, 0) AS absence_events_ytd
FROM gold_employees_current e
LEFT JOIN absences_ytd ab
  ON e.employee_id = ab.employee_id
WHERE e.status = 'Active';
