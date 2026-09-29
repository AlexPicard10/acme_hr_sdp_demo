# Pipeline LDP — HR Common Base

Ordre du graphe (le pipeline résout les dépendances automatiquement) :

```
00-Data-Sources/
  DLT-Raw-Employees.sql   -> bronze_employees   (Streaming Table + Auto Loader Volume UC)
  DLT-Raw-Absences.sql    -> bronze_absences    (Streaming Table + Auto Loader Volume UC)

01-Employees/
  DLT-Employees-Silver.sql -> silver_employees          (Streaming Table + Expectations + join départements)
  DLT-Employees-Gold.sql   -> gold_employees_current    (Auto CDC — SCD Type 1, état courant = socle)
                              gold_employees_history     (Auto CDC — SCD Type 2, historique mobilité)

02-Absences/
  DLT-Absences-Silver.sql  -> silver_absences            (Streaming Table + Expectations, motif libre `comment`)
  DLT-Gold-Views.sql       -> gold_hr_common_base        (View persistante UC)
```

Toutes les tables persistées sont en `CLUSTER BY AUTO` (liquid clustering automatique).

Les agrégats de consommation (Materialized Views) sont construits **en no-code** hors pipeline, dans
le Visual Data Prep `../02-Visual-Data-Prep/`, qui lit les tables silver/gold produites ici.

`03-Explorations/` n'est **pas** inclus dans le pipeline (requêtes de validation à lancer sur un
SQL Warehouse).

Fonctionnalités LDP illustrées : **Streaming Table**, **Auto Loader** (`STREAM read_files`),
**Expectations**, **Auto CDC** (SCD Type 1 & 2), **View**, **CLUSTER BY AUTO**.
