-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 1bis / Ingestion des événements d'Absence (Auto Loader depuis un Volume UC)
-- MAGIC #### Deuxième source du socle : un flux d'événements d'absence (maladie, congés, formation…).
-- MAGIC Même pattern que les employés : un dossier dédié du Volume, ingéré en streaming. Les nouveaux
-- MAGIC fichiers déposés au fil de l'eau sont automatiquement pris en compte au run suivant.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_absences
COMMENT "Événements bruts d'absence ingérés depuis un Volume UC via Auto Loader — couche bronze"
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT
  *,
  _metadata.file_path      AS _source_file,
  current_timestamp()      AS _ingested_at
FROM STREAM read_files(
  '${absences_path}',
  format => 'json',
  inferColumnTypes => true,
  schemaEvolutionMode => 'rescue'
);
