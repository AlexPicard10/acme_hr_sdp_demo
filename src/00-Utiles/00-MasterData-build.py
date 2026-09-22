# Databricks notebook source
# MAGIC %md
# MAGIC # HR Common Base — Setup (schemas, Volume) + Build du master data (référentiel Départements)
# MAGIC
# MAGIC À exécuter **une fois** avant le pipeline LDP. Crée les schemas, le **Volume UC** qui reçoit les
# MAGIC données brutes HR (déposées par `scripts/generate-hr-data.py`) et le référentiel stable
# MAGIC `departments` (joint en couche silver). Équivalent d'un dataset "référentiel" Dataiku.
# MAGIC
# MAGIC > Le catalog `alp_demo_catalog` existe déjà (créé côté votre workspace Databricks).
# MAGIC > Le `department_id` ici doit correspondre à celui généré dans les extraits employés
# MAGIC > (`scripts/generate-hr-data.py`).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Un schéma unique par démo (alp_demo_catalog accueillera d'autres démos).
# MAGIC CREATE SCHEMA IF NOT EXISTS alp_demo_catalog.acme_hr;
# MAGIC -- Volume managé qui reçoit les extraits HR bruts (un dossier par dataset : employees/, absences/)
# MAGIC CREATE VOLUME IF NOT EXISTS alp_demo_catalog.acme_hr.landing;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Référentiel Départements
# MAGIC 12 départements HR répartis sur des Business Units et régions (données fictives).

# COMMAND ----------

departments = [
    # (id,     name,                       business_unit,           region,          country,   cost_center)
    ("D001",  "Direction Générale",        "Corporate",             "Île-de-France", "France",  "CC-1000"),
    ("D002",  "Ressources Humaines",       "Corporate",             "Île-de-France", "France",  "CC-1100"),
    ("D003",  "Finance & Contrôle",        "Corporate",             "Île-de-France", "France",  "CC-1200"),
    ("D004",  "Systèmes d'Information",    "Corporate",             "Hauts-de-France","France", "CC-1300"),
    ("D005",  "Exploitation Réseaux",      "Networks",              "Auvergne-Rhône-Alpes", "France", "CC-2000"),
    ("D006",  "Maintenance Industrielle",  "Networks",              "Grand Est",     "France",  "CC-2100"),
    ("D007",  "Énergies Renouvelables",    "Renewables",            "Occitanie",     "France",  "CC-3000"),
    ("D008",  "Trading & Marchés",         "Global Energy Mgmt",    "Île-de-France", "France",  "CC-4000"),
    ("D009",  "Commercial B2B",            "Retail",                "Provence-Alpes-Côte d'Azur", "France", "CC-5000"),
    ("D010",  "Relation Client B2C",       "Retail",                "Nouvelle-Aquitaine", "France", "CC-5100"),
    ("D011",  "Recherche & Innovation",    "Research",              "Île-de-France", "France",  "CC-6000"),
    ("D012",  "Opérations Benelux",        "International",         "Bruxelles",     "Belgique","CC-7000"),
]

columns = ["department_id", "department_name", "business_unit", "region", "country", "cost_center"]

df = spark.createDataFrame(departments, columns)

(df.write
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable("alp_demo_catalog.acme_hr.departments"))

spark.sql("ALTER TABLE alp_demo_catalog.acme_hr.departments "
          "SET TBLPROPERTIES ('comment' = 'Référentiel départements HR (master data)')")

display(spark.table("alp_demo_catalog.acme_hr.departments"))
