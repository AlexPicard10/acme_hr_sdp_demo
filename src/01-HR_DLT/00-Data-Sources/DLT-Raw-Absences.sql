-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 1bis / Ingestion des événements d'Absence des 2 filiales (Auto Loader + Append Flows)
-- MAGIC #### Deuxième source : un flux d'événements d'absence (maladie, congés, formation…).
-- MAGIC Même pattern que les employés : chaque filiale dépose ses fichiers dans son Volume, une seule
-- MAGIC table cible, **un Append Flow par filiale**. Les nouveaux fichiers déposés au fil de l'eau sont
-- MAGIC automatiquement pris en compte au run suivant.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_absences
CLUSTER BY AUTO
COMMENT "Événements bruts d'absence des 2 filiales (FR, BE), concaténés par Append Flows depuis 2 Volumes UC — couche bronze"
TBLPROPERTIES ('quality' = 'bronze');

-- COMMAND ----------

-- Flow 1 : ACME France (Volume landing_fr)
CREATE FLOW bronze_absences_fr
AS INSERT INTO bronze_absences BY NAME
SELECT
  *,
  'FR'                AS source_entity,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${absences_path_fr}',
  format => 'json',
  inferColumnTypes => true,
  schemaEvolutionMode => 'rescue'
);

-- COMMAND ----------

-- Flow 2 : ACME Belgique (Volume landing_be)
CREATE FLOW bronze_absences_be
AS INSERT INTO bronze_absences BY NAME
SELECT
  *,
  'BE'                AS source_entity,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${absences_path_be}',
  format => 'json',
  inferColumnTypes => true,
  schemaEvolutionMode => 'rescue'
);
