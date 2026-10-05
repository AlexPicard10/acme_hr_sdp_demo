# 2 / Préparation & Qualité — Employés (Streaming Table + Expectations)
# Fichier SQL jumeau : 01-HR_SDP_SQL/01-Employees/DLT-Employees-Silver.sql
#
#   SQL                                              Python
#   CONSTRAINT c EXPECT (…) ON VIOLATION FAIL UPDATE @dp.expect_or_fail("c", "…")
#   CONSTRAINT c EXPECT (…) ON VIOLATION DROP ROW    @dp.expect_all_or_drop({"c": "…", …})
#   CONSTRAINT c EXPECT (…)                          @dp.expect_all({"c": "…", …})   (warn)
#   WITH typed AS (… FROM STREAM(bronze_employees))  typed = spark.readStream.table("bronze_employees")…
#   CASE WHEN … END                                  F.when(…).when(…).otherwise(…)
#
# Les conditions des Expectations restent des expressions SQL (chaînes), mêmes noms qu'en SQL :
# les métriques qualité des deux pipelines se comparent directement.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

MASTER_DATA_SCHEMA = spark.conf.get("master_data_schema")
# Même littéral que le SQL (365.25 est un DECIMAL(5,2)) : types et arrondis identiques.
DAYS_PER_YEAR = F.lit(365.25).cast("decimal(5,2)")


@dp.table(
    name="silver_employees",
    comment="Employés des 2 filiales nettoyés, typés, enrichis (age_bracket, ancienneté) et joints au référentiel départements — couche silver",
    table_properties={"quality": "silver"},
    cluster_by_auto=True,
)
@dp.expect_or_fail("valid_employee_id", "employee_id IS NOT NULL")
@dp.expect_all_or_drop({
    "valid_birth_date": "birth_date IS NOT NULL AND birth_date > DATE'1940-01-01'",
    "valid_hire_date": "hire_date IS NOT NULL AND hire_date >= birth_date",
    "plausible_age": "age BETWEEN 16 AND 75",
    "valid_entity": "source_entity IN ('FR','BE')",
})
@dp.expect_all({
    "known_department": "department_name IS NOT NULL",
    "valid_contract": "contract_type IN ('CDI','CDD','Alternance','Stage','Intérim')",
})
def silver_employees():
    # Étape 1 : typage et colonnes dérivées des extraits bruts (= CTE `typed` en SQL)
    typed = spark.readStream.table("bronze_employees").select(
        "source_entity",                                                # filiale d'origine (FR / BE)
        "employee_id",
        F.to_date("extract_date").alias("extract_date"),
        F.to_timestamp("extract_ts").alias("extract_ts"),               # ordonne les fichiers entre eux
        F.to_timestamp("updated_at").alias("updated_at"),               # ordonne les versions dans un fichier
        "_source_file",                                                 # fichier d'origine
        F.initcap("first_name").alias("first_name"),
        F.upper("last_name").alias("last_name"),
        "gender",
        F.to_date("birth_date").alias("birth_date"),
        F.floor(F.datediff(F.current_date(), F.to_date("birth_date")) / DAYS_PER_YEAR).cast("int").alias("age"),
        F.to_date("hire_date").alias("hire_date"),
        F.round(F.datediff(F.current_date(), F.to_date("hire_date")) / DAYS_PER_YEAR, 1).alias("seniority_years"),
        "job_title",
        "contract_type",
        "work_location",
        "manager_id",
        F.lower("email").alias("email"),
        F.col("fte").cast("double").alias("fte"),
        "status",
        "department_id",
    )

    # Référentiel départements (master data) : lecture batch, jointure stream-static
    departments = spark.read.table(f"{MASTER_DATA_SCHEMA}.departments")

    # Étape 2 : jointure au référentiel + tranches calculées
    return (
        typed.alias("t")
        .join(departments.alias("d"), F.col("t.department_id") == F.col("d.department_id"), "left")
        .select(
            "t.*",
            "d.department_name",
            "d.business_unit",
            "d.region",
            "d.country",
            "d.cost_center",
            F.when(F.col("t.age") < 20, "<20")
            .when(F.col("t.age") < 35, "20-34")
            .when(F.col("t.age") < 45, "35-44")
            .when(F.col("t.age") < 55, "45-54")
            .otherwise("55+")
            .alias("age_bracket"),
            F.when(F.col("t.seniority_years") < 2, "0-2 ans")
            .when(F.col("t.seniority_years") < 5, "2-5 ans")
            .when(F.col("t.seniority_years") < 10, "5-10 ans")
            .when(F.col("t.seniority_years") < 20, "10-20 ans")
            .otherwise("20+ ans")
            .alias("seniority_bracket"),
        )
    )
