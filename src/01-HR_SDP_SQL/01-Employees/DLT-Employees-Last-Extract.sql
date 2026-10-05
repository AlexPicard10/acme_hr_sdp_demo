-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 2bis / Contrôle de la dernière livraison — Window functions (Materialized View)
-- MAGIC #### Pour le dernier fichier livré par chaque filiale : une ligne par employé, la plus récente.
-- MAGIC Un extrait peut contenir **plusieurs lignes pour un même `employee_id`** : le SIRH exporte toutes
-- MAGIC les versions saisies dans la journée (ex. un poste saisi par erreur à 7 h, corrigé à 9 h), horodatées
-- MAGIC par `updated_at`. On veut voir ce que contient la **dernière livraison**, dédupliquée.
-- MAGIC
-- MAGIC **La fonction de fenêtre, pas à pas**
-- MAGIC
-- MAGIC ```sql
-- MAGIC ROW_NUMBER() OVER (PARTITION BY source_entity, employee_id ORDER BY updated_at DESC)
-- MAGIC ```
-- MAGIC 1. `PARTITION BY` : regroupe les lignes d'un même employé (dans une même filiale) ;
-- MAGIC 2. `ORDER BY updated_at DESC` : trie ses versions de la plus récente à la plus ancienne ;
-- MAGIC 3. `ROW_NUMBER()` : numérote 1, 2, 3… dans chaque groupe ; on garde le **rang 1**.
-- MAGIC
-- MAGIC Raccourci équivalent : `QUALIFY ROW_NUMBER() OVER (…) = 1` directement après le `FROM`.
-- MAGIC D'autres fonctions de fenêtre enrichissent le contrôle : `COUNT(*) OVER` (nombre de versions dans
-- MAGIC le fichier) et `LAG()` (valeur de la version précédente : ce que la correction a changé).
-- MAGIC
-- MAGIC **Pourquoi une Materialized View ?** Les fonctions de fenêtre de type `ROW_NUMBER` / `LAG` ne
-- MAGIC sont **pas supportées sur une streaming table** (un flux n'a pas de « fin » sur laquelle trier).
-- MAGIC On les écrit donc dans une **Materialized View**, qui lit `silver_employees` en batch.
-- MAGIC
-- MAGIC **Auto CDC ou window function : quand utiliser quoi**
-- MAGIC
-- MAGIC | | Auto CDC (`gold_employees_current`) | Window function (cette MV) |
-- MAGIC |---|---|---|
-- MAGIC | Résultat | dernière version **connue** de chaque employé, tous fichiers confondus | ce que dit la requête : ici, le **dernier fichier** seulement |
-- MAGIC | Coût par run | ne traite que les **nouvelles lignes** (incrémental) | relit la table source (recalcul, incrémental seulement dans certains cas) |
-- MAGIC | Flexibilité | une logique : dernière version (SCD1) ou historique (SCD2) | toute logique : rang, top N, version précédente (`LAG`), filtres… |
-- MAGIC | Type de table | streaming table | Materialized View (batch) |
-- MAGIC | À utiliser pour | la chaîne de production « état courant » | contrôles, analyses, règles plus riches qu'un simple « dernier » |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_employees_last_extract
CLUSTER BY AUTO
COMMENT "Dernier fichier livré par chaque filiale, une ligne par employé (window functions ROW_NUMBER, COUNT, LAG)"
TBLPROPERTIES ('quality' = 'silver')
AS
WITH last_file AS (
  -- Le fichier le plus récent de chaque filiale (max_by : la valeur de _source_file au extract_ts maximal)
  SELECT
    source_entity,
    max_by(_source_file, extract_ts) AS last_source_file
  FROM silver_employees
  GROUP BY source_entity
),
ranked AS (
  SELECT
    e.*,
    -- Combien de versions de cet employé dans le fichier ?
    COUNT(*) OVER (PARTITION BY e.source_entity, e.employee_id) AS versions_in_file,
    -- Valeurs de la version précédente (NULL s'il n'y en a qu'une) : ce que la correction a changé
    LAG(e.department_id) OVER (PARTITION BY e.source_entity, e.employee_id ORDER BY e.updated_at) AS previous_department_id,
    LAG(e.job_title)     OVER (PARTITION BY e.source_entity, e.employee_id ORDER BY e.updated_at) AS previous_job_title,
    -- Rang 1 = la version la plus récente
    ROW_NUMBER()         OVER (PARTITION BY e.source_entity, e.employee_id ORDER BY e.updated_at DESC) AS rn
  FROM silver_employees e
  INNER JOIN last_file l
    ON  e.source_entity = l.source_entity
    AND e._source_file  = l.last_source_file
)
SELECT * EXCEPT (rn)
FROM ranked
WHERE rn = 1;
