# HR 360 — Lakeflow Declarative Pipeline en PYTHON
# Même logique que le pipeline SQL (src/01-HR_SDP_SQL), publiée dans le schéma acme_hr_python.
#
# 1 / Ingestion des extraits Employés des 2 filiales (Auto Loader + Append Flows)
# Fichier SQL jumeau : 01-HR_SDP_SQL/00-Data-Sources/DLT-Raw-Employees.sql
#
#   SQL                                              Python
#   CREATE OR REFRESH STREAMING TABLE t (sans AS)    dp.create_streaming_table("t")
#   CREATE FLOW f AS INSERT INTO t BY NAME …         @dp.append_flow(target="t", name="f")
#   STREAM read_files('${employees_path_fr}', …)     spark.readStream.format("cloudFiles").load(spark.conf.get(...))
#
# Point fort du Python : les flows sont générés par une boucle. Ajouter une 3ᵉ filiale = ajouter
# une entrée dans le dictionnaire SOURCES (et sa clé de configuration), sans full refresh.

from pyspark import pipelines as dp

from utilities.ingestion import read_hr_extract

# Filiale -> clé de configuration du pipeline qui contient le chemin de son Volume
SOURCES = {
    "FR": "employees_path_fr",
    "BE": "employees_path_be",
}

# Table cible unique : son schéma est déduit des flows qui l'alimentent.
dp.create_streaming_table(
    name="bronze_employees",
    comment="Extraits bruts Employés des 2 filiales (FR, BE), concaténés par Append Flows depuis 2 Volumes UC — couche bronze",
    table_properties={"quality": "bronze"},
    cluster_by_auto=True,
)


def add_entity_flow(entity: str, path: str) -> None:
    """Déclare un Append Flow pour une filiale (une fonction par flow : entity et path sont figés)."""

    @dp.append_flow(target="bronze_employees", name=f"bronze_employees_{entity.lower()}")
    def _flow():
        return read_hr_extract(spark, path, entity)


for entity, conf_key in SOURCES.items():
    add_entity_flow(entity, spark.conf.get(conf_key))
