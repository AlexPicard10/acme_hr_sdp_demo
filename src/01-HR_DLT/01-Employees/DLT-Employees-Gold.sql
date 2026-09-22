-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 3 / Consolidation — Employés (Auto CDC, SCD Type 1)
-- MAGIC #### Équivalent de la zone *dédup / dernière version* : on ne garde que l'état courant par employé.
-- MAGIC `CREATE FLOW … AS AUTO CDC INTO … STORED AS SCD TYPE 1` applique les changements et conserve, pour
-- MAGIC chaque `employee_gid`, la **dernière valeur** (ordonnée par `extract_ts`, l'instant d'extraction). C'est le **socle
-- MAGIC commun** "état courant" : une ligne par employé, toujours à jour, alimentée par les extraits quotidiens.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_employees_current
COMMENT "Socle commun HR — état courant : dernière version connue de chaque employé (SCD1)"
TBLPROPERTIES (
  'quality' = 'gold',
  'delta.enableChangeDataFeed' = 'true'
);

CREATE FLOW gold_employees_current_flow
AS AUTO CDC INTO
  gold_employees_current
FROM STREAM(silver_employees)
  KEYS (employee_gid)
  SEQUENCE BY extract_ts
  STORED AS SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4 / Historisation — Employés (Auto CDC, SCD Type 2)
-- MAGIC #### Conserver l'historique des changements de département, contrat ou poste dans le temps.
-- MAGIC `STORED AS SCD TYPE 2` ajoute les colonnes système `__START_AT` / `__END_AT`. `TRACK HISTORY ON`
-- MAGIC limite la création d'une nouvelle ligne d'historique aux seuls changements de mobilité
-- MAGIC (département / contrat / poste) — utile pour l'analyse RH (mobilité interne, ancienneté au poste).
-- MAGIC L'état courant se lit avec `WHERE __END_AT IS NULL`.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_employees_history
COMMENT "Historique employés (SCD2) — mobilité département / contrat / poste dans le temps"
TBLPROPERTIES ('quality' = 'gold');

CREATE FLOW gold_employees_history_flow
AS AUTO CDC INTO
  gold_employees_history
FROM STREAM(silver_employees)
  KEYS (employee_gid)
  SEQUENCE BY extract_ts
  STORED AS SCD TYPE 2
  TRACK HISTORY ON department_id, contract_type, job_title;