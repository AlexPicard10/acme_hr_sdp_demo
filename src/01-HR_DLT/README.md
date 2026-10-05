# Pipeline LDP — HR 360

Ordre du graphe (le pipeline résout les dépendances automatiquement) :

```
00-Data-Sources/
  DLT-Raw-Employees.sql   -> bronze_employees   (Streaming Table + Auto Loader, 2 Append Flows : landing_fr + landing_be)
  DLT-Raw-Absences.sql    -> bronze_absences    (Streaming Table + Auto Loader, 2 Append Flows : landing_fr + landing_be)

01-Employees/
  DLT-Employees-Silver.sql -> silver_employees          (Streaming Table + Expectations + join départements)
  DLT-Employees-Last-Extract.sql -> silver_employees_last_extract (Materialized View + window functions ROW_NUMBER / LAG)
  DLT-Employees-Gold.sql   -> gold_employees_current    (Auto CDC — SCD Type 1, état courant, SEQUENCE BY STRUCT(extract_ts, updated_at))
                              gold_employees_history     (Auto CDC — SCD Type 2, historique mobilité)

02-Absences/
  DLT-Absences-Silver.sql  -> silver_absences            (Streaming Table + Expectations, motif libre `comment`)
  DLT-Gold-Employee-360.sql -> gold_employee_360        (Materialized View)
```

Toutes les tables persistées sont en `CLUSTER BY AUTO` (liquid clustering automatique).

Les agrégats de consommation (Materialized Views) sont construits **en no-code** hors pipeline, dans
le Visual Data Prep `../02-Visual-Data-Prep/`, qui lit les tables silver/gold produites ici.

`03-Explorations/` n'est **pas** inclus dans le pipeline (requêtes de validation à lancer sur un
SQL Warehouse).

Fonctionnalités LDP illustrées : **Streaming Table**, **Auto Loader** (`STREAM read_files`),
**Append Flows** (concaténation multi-sources), **Expectations**, **window functions** dans une
Materialized View, **Auto CDC** (SCD Type 1 & 2), **Materialized View**, **CLUSTER BY AUTO**.
