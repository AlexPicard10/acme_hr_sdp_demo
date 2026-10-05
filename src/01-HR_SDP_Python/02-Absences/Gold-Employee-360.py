# 6 / Vue 360° employé — Materialized View de consommation (Unity Catalog)
# Fichier SQL jumeau : 01-HR_SDP_SQL/02-Absences/DLT-Gold-Employee-360.sql
#
# spark.read.table(...) (lecture batch) dans un @dp.materialized_view : le résultat est stocké et
# rafraîchi à chaque update du pipeline.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_employee_360",
    comment="Vue 360° employé — consolidation (état courant) + KPI d'absence de l'année en cours",
    table_properties={"quality": "gold"},
    cluster_by_auto=True,
)
def gold_employee_360():
    # Étape 1 : KPI d'absence de l'année en cours, par employé (= CTE `absences_ytd` en SQL)
    absences_ytd = (
        spark.read.table("silver_absences")
        .filter(F.year("start_date") == F.year(F.current_date()))
        .groupBy("employee_id")
        .agg(
            F.sum("days").alias("absence_days_ytd"),
            F.count("*").alias("absence_events_ytd"),
        )
    )

    # Étape 2 : état courant de chaque employé actif, enrichi des KPI
    employees = spark.read.table("gold_employees_current")
    return (
        employees.alias("e")
        .join(absences_ytd.alias("ab"), F.col("e.employee_id") == F.col("ab.employee_id"), "left")
        .filter(F.col("e.status") == "Active")
        .select(
            "e.*",
            F.coalesce(F.col("ab.absence_days_ytd"), F.lit(0)).alias("absence_days_ytd"),
            F.coalesce(F.col("ab.absence_events_ytd"), F.lit(0)).alias("absence_events_ytd"),
        )
    )
