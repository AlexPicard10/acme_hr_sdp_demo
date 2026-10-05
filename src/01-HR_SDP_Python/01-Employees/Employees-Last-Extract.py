# 2bis / Contrôle de la dernière livraison — Window functions (Materialized View)
# Fichier SQL jumeau : 01-HR_SDP_SQL/01-Employees/DLT-Employees-Last-Extract.sql
# (explications détaillées et tableau « Auto CDC ou window function » dans le fichier SQL)
#
#   SQL                                                   Python
#   CREATE OR REFRESH MATERIALIZED VIEW                   @dp.materialized_view(...)
#   … OVER (PARTITION BY a, b ORDER BY c DESC)            .over(Window.partitionBy("a", "b").orderBy(F.col("c").desc()))
#   ROW_NUMBER() / COUNT(*) / LAG(x)                      F.row_number() / F.count("*") / F.lag("x")
#   max_by(_source_file, extract_ts)                      F.max_by("_source_file", "extract_ts")
#
# En Python, la fenêtre est un objet réutilisable : on la déclare une fois et on l'applique à
# plusieurs colonnes. Les window functions restent interdites sur une streaming table : on lit
# silver_employees en batch (spark.read), d'où la Materialized View.

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.materialized_view(
    name="silver_employees_last_extract",
    comment="Dernier fichier livré par chaque filiale, une ligne par employé (window functions ROW_NUMBER, COUNT, LAG)",
    table_properties={"quality": "silver"},
    cluster_by_auto=True,
)
def silver_employees_last_extract():
    employees = spark.read.table("silver_employees")

    # Le fichier le plus récent de chaque filiale (= CTE `last_file` en SQL)
    last_file = employees.groupBy("source_entity").agg(
        F.max_by("_source_file", "extract_ts").alias("last_source_file")
    )

    # Les fenêtres : toutes les versions d'un employé dans une filiale
    per_employee = Window.partitionBy("source_entity", "employee_id")
    per_employee_oldest_first = per_employee.orderBy("updated_at")
    per_employee_latest_first = per_employee.orderBy(F.col("updated_at").desc())

    # Les lignes du dernier fichier (on ne garde que les colonnes de silver_employees : après la
    # jointure, `source_entity` existe des deux côtés et la fenêtre serait ambiguë)
    in_last_file = (
        employees.alias("e")
        .join(
            last_file.alias("l"),
            (F.col("e.source_entity") == F.col("l.source_entity"))
            & (F.col("e._source_file") == F.col("l.last_source_file")),
            "inner",
        )
        .select("e.*")
    )

    # = CTE `ranked` en SQL
    ranked = in_last_file.select(
        "*",
        F.count("*").over(per_employee).alias("versions_in_file"),
        F.lag("department_id").over(per_employee_oldest_first).alias("previous_department_id"),
        F.lag("job_title").over(per_employee_oldest_first).alias("previous_job_title"),
        F.row_number().over(per_employee_latest_first).alias("rn"),
    )

    # Rang 1 = la version la plus récente
    return ranked.filter(F.col("rn") == 1).drop("rn")
