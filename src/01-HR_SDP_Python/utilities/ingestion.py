"""Fonctions partagées par les fichiers bronze du pipeline Python.

Ce module n'est PAS une source du pipeline (il est hors des globs de `libraries`) : c'est du code
Python ordinaire, importé par les fichiers du pipeline grâce à `root_path` (le dossier
`01-HR_SDP_Python` est ajouté au `sys.path`). C'est l'un des atouts du Python : la logique
d'ingestion est écrite une fois et réutilisée pour chaque dataset et chaque filiale.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_hr_extract(spark: SparkSession, path: str, entity: str) -> DataFrame:
    """Lit en streaming (Auto Loader) les extraits JSON d'une filiale et ajoute les colonnes techniques.

    Équivalent SQL : `SELECT *, '<entity>' AS source_entity, _metadata.file_path AS _source_file, …
    FROM STREAM read_files('<path>', format => 'json', inferColumnTypes => true,
    schemaEvolutionMode => 'rescue')`.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(path)
        .select(
            "*",
            F.lit(entity).alias("source_entity"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_source_file_ts"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
