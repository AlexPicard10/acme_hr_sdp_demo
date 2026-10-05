# Pipeline LDP — HR 360 (Python)

Pipeline `HR_360_Python_<target>` → schéma `alp_demo_catalog.acme_hr_python`. C'est la **même
logique** que le pipeline SQL [`../01-HR_SDP_SQL/`](../01-HR_SDP_SQL/README.md), écrite avec l'API
Python `from pyspark import pipelines as dp`. Les deux pipelines lisent les mêmes Volumes
(`acme_hr.landing_fr`, `acme_hr.landing_be`) et produisent des tables de mêmes noms, mêmes colonnes,
même contenu (vérification : section « SQL vs Python » de
`../01-HR_SDP_SQL/03-Explorations/hr_exploration.sql`).

```
00-Data-Sources/
  Raw-Employees.py           -> bronze_employees   (create_streaming_table + 2 @dp.append_flow générés par une boucle)
  Raw-Absences.py            -> bronze_absences    (idem)

01-Employees/
  Employees-Silver.py        -> silver_employees               (@dp.table + @dp.expect* + join départements)
  Employees-Last-Extract.py  -> silver_employees_last_extract  (@dp.materialized_view + Window : row_number / count / lag)
  Employees-Gold.py          -> gold_employees_current         (create_auto_cdc_flow, SCD Type 1)
                                gold_employees_history         (create_auto_cdc_flow, SCD Type 2)

02-Absences/
  Absences-Silver.py         -> silver_absences    (@dp.table + @dp.expect*)
  Gold-Employee-360.py       -> gold_employee_360  (@dp.materialized_view)

utilities/
  ingestion.py               fonction Auto Loader partagée (importée grâce à root_path, hors pipeline)
```

Chaque fichier indique en tête son fichier SQL jumeau ; les explications métier détaillées sont dans
les notebooks SQL.

## Équivalences SQL ↔ Python

| Concept | SQL | Python |
|---|---|---|
| Streaming table | `CREATE OR REFRESH STREAMING TABLE t AS SELECT … FROM STREAM(src)` | `@dp.table()` qui renvoie `spark.readStream.table("src")…` |
| Materialized view | `CREATE OR REFRESH MATERIALIZED VIEW v AS SELECT … FROM src` | `@dp.materialized_view()` qui renvoie `spark.read.table("src")…` |
| Table cible vide (pour des flows) | `CREATE OR REFRESH STREAMING TABLE t;` | `dp.create_streaming_table("t")` |
| Append Flow | `CREATE FLOW f AS INSERT INTO t BY NAME SELECT …` | `@dp.append_flow(target="t", name="f")` |
| Auto Loader | `STREAM read_files('path', format => 'json', …)` | `spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")…load(path)` |
| Paramètre du pipeline | `'${employees_path_fr}'` | `spark.conf.get("employees_path_fr")` |
| Expectation (warn / drop / fail) | `CONSTRAINT c EXPECT (cond) [ON VIOLATION DROP ROW \| FAIL UPDATE]` | `@dp.expect` / `@dp.expect_or_drop` / `@dp.expect_or_fail` (et `expect_all*` pour un dict) |
| Auto CDC | `CREATE FLOW f AS AUTO CDC INTO t FROM STREAM(src) KEYS (k) SEQUENCE BY STRUCT(a, b) STORED AS SCD TYPE 2 TRACK HISTORY ON x, y` | `dp.create_auto_cdc_flow(target="t", source="src", keys=["k"], sequence_by=F.struct("a", "b"), stored_as_scd_type=2, track_history_column_list=["x", "y"])` |
| Window function | `ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC)` | `F.row_number().over(Window.partitionBy("a").orderBy(F.col("b").desc()))` |
| Étapes intermédiaires | CTE `WITH typed AS (…)` | variables DataFrame `typed = …` |
| Liquid clustering auto | `CLUSTER BY AUTO` | `cluster_by_auto=True` |

## Quand préférer Python ?

- **Factoriser** : une fonction d'ingestion (`utilities/ingestion.py`) réutilisée par tous les
  datasets ; des flows générés par une boucle (ajouter une filiale = une entrée dans `SOURCES`).
- **Tester** : les transformations sont des fonctions Python, testables unitairement sur un petit
  DataFrame.
- **Fonctionnalités réservées à Python** : sinks (Kafka, tables externes), `foreach_batch_sink`,
  Auto CDC depuis des snapshots, sources de données personnalisées.

Pour une équipe habituée au SQL et des transformations déclaratives simples, le SQL reste plus
lisible : les deux langages s'exécutent sur le même moteur, avec les mêmes performances.
