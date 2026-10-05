# 3 & 4 / Consolidation et historisation — Employés (Auto CDC, SCD Type 1 et 2)
# Fichier SQL jumeau : 01-HR_SDP_SQL/01-Employees/DLT-Employees-Gold.sql
#
#   SQL                                              Python
#   CREATE OR REFRESH STREAMING TABLE t;             dp.create_streaming_table("t", …)
#   CREATE FLOW f AS AUTO CDC INTO t                 dp.create_auto_cdc_flow(target="t", name="f",
#     FROM STREAM(silver_employees)                    source="silver_employees",
#     KEYS (employee_id)                               keys=["employee_id"],
#     SEQUENCE BY STRUCT(extract_ts, updated_at)       sequence_by=F.struct("extract_ts", "updated_at"),
#     STORED AS SCD TYPE 2                             stored_as_scd_type=2,
#     TRACK HISTORY ON a, b                            track_history_column_list=["a", "b"])
#
# Pas de décorateur ici : la source est passée par son NOM (pas un DataFrame), Auto CDC la lit
# en streaming. SEQUENCE BY STRUCT(extract_ts, updated_at) = l'équivalent streaming et incrémental
# du ROW_NUMBER (voir Employees-Last-Extract.py).

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SEQUENCE = F.struct("extract_ts", "updated_at")

# --- SCD Type 1 : état courant, dernière version connue de chaque employé ---
dp.create_streaming_table(
    name="gold_employees_current",
    comment="Employés — état courant : dernière version connue de chaque employé (SCD1)",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
    cluster_by_auto=True,
)

dp.create_auto_cdc_flow(
    name="gold_employees_current_flow",
    target="gold_employees_current",
    source="silver_employees",
    keys=["employee_id"],
    sequence_by=SEQUENCE,
    stored_as_scd_type=1,
)

# --- SCD Type 2 : historique des mobilités (département / contrat / poste) ---
dp.create_streaming_table(
    name="gold_employees_history",
    comment="Historique employés (SCD2) — mobilité département / contrat / poste dans le temps",
    table_properties={"quality": "gold"},
    cluster_by_auto=True,
)

dp.create_auto_cdc_flow(
    name="gold_employees_history_flow",
    target="gold_employees_history",
    source="silver_employees",
    keys=["employee_id"],
    sequence_by=SEQUENCE,
    stored_as_scd_type=2,
    track_history_column_list=["department_id", "contract_type", "job_title"],
)
