# 1bis / Ingestion des événements d'Absence des 2 filiales (Auto Loader + Append Flows)
# Fichier SQL jumeau : 01-HR_SDP_SQL/00-Data-Sources/DLT-Raw-Absences.sql
#
# Même pattern que les employés, et même fonction d'ingestion (utilities/ingestion.py) :
# une table cible, un Append Flow par filiale.

from pyspark import pipelines as dp

from utilities.ingestion import read_hr_extract

SOURCES = {
    "FR": "absences_path_fr",
    "BE": "absences_path_be",
}

dp.create_streaming_table(
    name="bronze_absences",
    comment="Événements bruts d'absence des 2 filiales (FR, BE), concaténés par Append Flows depuis 2 Volumes UC — couche bronze",
    table_properties={"quality": "bronze"},
    cluster_by_auto=True,
)


def add_entity_flow(entity: str, path: str) -> None:
    @dp.append_flow(target="bronze_absences", name=f"bronze_absences_{entity.lower()}")
    def _flow():
        return read_hr_extract(spark, path, entity)


for entity, conf_key in SOURCES.items():
    add_entity_flow(entity, spark.conf.get(conf_key))
