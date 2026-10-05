-- Databricks notebook source
-- MAGIC %md
-- MAGIC # HR 360 — Lakeflow Declarative Pipeline (LDP)
-- MAGIC ### Asset d'enablement — montée en compétence des équipes HR Data sur Databricks SDP (société fictive ACME)
-- MAGIC
-- MAGIC Ce pipeline construit la **vue 360° des employés** : les extraits du SIRH
-- MAGIC de deux filiales (ACME France et ACME Belgique) atterrissent chacun dans **son propre Volume
-- MAGIC Unity Catalog** (`landing_fr`, `landing_be`), sont ingérés en **Auto Loader**, concaténés, puis
-- MAGIC consolidés en une table employés unique.
-- MAGIC
-- MAGIC | Étape | Fonctionnalité Lakeflow (SDP) |
-- MAGIC |---|---|
-- MAGIC | Ingestion des extraits (Volume UC) | **Streaming Table** + **Auto Loader** (`STREAM read_files`) — couche *bronze* |
-- MAGIC | Concaténation des 2 filiales | **Append Flows** (`CREATE FLOW … INSERT INTO`) — couche *bronze* |
-- MAGIC | Nettoyage, typage, colonnes calculées | **Streaming Table** + **Expectations** — couche *silver* |
-- MAGIC | Jointure référentiel | jointure SQL sur le master data |
-- MAGIC | Dernière ligne par employé dans le dernier fichier | **Window function** `ROW_NUMBER` dans une **Materialized View** |
-- MAGIC | Déduplication / historisation | **Auto CDC** (SCD1/SCD2) — couche *gold* |
-- MAGIC | Sortie / consommation | **Materialized View** (Unity Catalog) |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1 / Ingestion des extraits Employés des 2 filiales (Auto Loader + Append Flows)
-- MAGIC #### `STREAM read_files(...)` détecte et ingère automatiquement les nouveaux fichiers JSON déposés dans un Volume.
-- MAGIC Auto Loader gère l'inférence de schéma, l'évolution de schéma (mode `rescue`) et le suivi
-- MAGIC incrémental des fichiers : le premier run charge l'historique, les suivants n'ingèrent que
-- MAGIC les nouveaux fichiers (traitement Delta, rapide et peu coûteux).
-- MAGIC
-- MAGIC **Concaténer deux sources de même structure : les Append Flows.** ACME France et ACME Belgique
-- MAGIC livrent le même format dans deux stockages différents. On déclare **une** table cible
-- MAGIC (`CREATE OR REFRESH STREAMING TABLE` sans requête), puis **un flow par source**
-- MAGIC (`CREATE FLOW … AS INSERT INTO … BY NAME`). Chaque flow a son propre suivi de fichiers, et la
-- MAGIC colonne `source_entity` indique la filiale d'origine.
-- MAGIC
-- MAGIC | | Append Flows (ce pipeline) | `UNION ALL` de deux streams |
-- MAGIC |---|---|---|
-- MAGIC | Ajouter une 3ᵉ filiale | un `CREATE FLOW` de plus, **sans** full refresh | la requête change : full refresh obligatoire |
-- MAGIC | Suivi des fichiers | un checkpoint par source, indépendant | un checkpoint unique pour toute la requête |
-- MAGIC | Lisibilité | une source = un bloc | une requête qui grossit |
-- MAGIC
-- MAGIC `BY NAME` aligne les colonnes **par nom** (et non par position) : un ordre de colonnes différent
-- MAGIC entre les deux SIRH ne pose pas de problème.
-- MAGIC
-- MAGIC `CLUSTER BY AUTO` active le **liquid clustering automatique** : Databricks choisit (et fait
-- MAGIC évoluer) les clés de clustering en fonction des requêtes réellement exécutées, via Predictive
-- MAGIC Optimization. Plus besoin de deviner des colonnes de partitionnement. Toutes les tables du
-- MAGIC pipeline l'utilisent.

-- COMMAND ----------

-- Table cible unique : son schéma est déduit des flows qui l'alimentent.
CREATE OR REFRESH STREAMING TABLE bronze_employees
CLUSTER BY AUTO
COMMENT "Extraits bruts Employés des 2 filiales (FR, BE), concaténés par Append Flows depuis 2 Volumes UC — couche bronze"
TBLPROPERTIES ('quality' = 'bronze');

-- COMMAND ----------

-- Flow 1 : ACME France (Volume landing_fr)
CREATE FLOW bronze_employees_fr
AS INSERT INTO bronze_employees BY NAME
SELECT
  *,
  'FR'                             AS source_entity,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _source_file_ts,
  current_timestamp()              AS _ingested_at
FROM STREAM read_files(
  '${employees_path_fr}',
  format => 'json',
  inferColumnTypes => true,
  schemaEvolutionMode => 'rescue'
);

-- COMMAND ----------

-- Flow 2 : ACME Belgique (Volume landing_be) — même structure, autre stockage
CREATE FLOW bronze_employees_be
AS INSERT INTO bronze_employees BY NAME
SELECT
  *,
  'BE'                             AS source_entity,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _source_file_ts,
  current_timestamp()              AS _ingested_at
FROM STREAM read_files(
  '${employees_path_be}',
  format => 'json',
  inferColumnTypes => true,
  schemaEvolutionMode => 'rescue'
);
