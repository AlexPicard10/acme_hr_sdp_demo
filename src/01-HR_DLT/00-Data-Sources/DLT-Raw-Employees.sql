-- Databricks notebook source
-- MAGIC %md
-- MAGIC # HR "Common Base" — Lakeflow Declarative Pipeline (LDP)
-- MAGIC ### Asset d'enablement — migration Dataiku ➜ Databricks (équipes HR (société fictive ACME))
-- MAGIC
-- MAGIC Ce pipeline reproduit le pattern du **socle commun HR** : les extraits HR (type CESAM/SESAM)
-- MAGIC atterrissent dans un **Volume Unity Catalog** (un dossier par dataset), sont ingérés en **Auto Loader**,
-- MAGIC puis consolidés en une table employés unique — l'équivalent Databricks du flow Dataiku.
-- MAGIC
-- MAGIC | Zone Dataiku | Équivalent Lakeflow (LDP) |
-- MAGIC |---|---|
-- MAGIC | Dataset d'entrée (CDH / S3) | **Streaming Table** + **Auto Loader** (`STREAM read_files`) — couche *bronze* |
-- MAGIC | Recipes *Prepare* (nettoyage, colonnes calculées) | **Streaming Table** + **Expectations** — couche *silver* |
-- MAGIC | Recipe *Join* (référentiels) | jointure SQL sur le master data |
-- MAGIC | Zone *Transform/Rename* + dédup | **Auto CDC** (SCD1/SCD2) — couche *gold* |
-- MAGIC | Datasets de sortie / consommation | **Materialized View** + **View** (Unity Catalog) |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1 / Ingestion des extraits Employés (Auto Loader depuis un Volume UC)
-- MAGIC #### `STREAM read_files(...)` détecte et ingère automatiquement les nouveaux fichiers JSON déposés dans le Volume.
-- MAGIC Auto Loader gère l'inférence de schéma, l'évolution de schéma (mode `rescue`) et le suivi
-- MAGIC incrémental des fichiers : le premier run charge l'historique, les suivants n'ingèrent que
-- MAGIC les nouveaux fichiers (traitement Delta, rapide et peu coûteux).

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_employees
COMMENT "Extraits bruts Employés (master HR) ingérés depuis un Volume UC via Auto Loader — couche bronze"
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT
  *,
  _metadata.file_path      AS _source_file,
  _metadata.file_modification_time AS _source_file_ts,
  current_timestamp()      AS _ingested_at
FROM STREAM read_files(
  '${employees_path}',
  format => 'json',
  inferColumnTypes => true,
  schemaEvolutionMode => 'rescue'
);
