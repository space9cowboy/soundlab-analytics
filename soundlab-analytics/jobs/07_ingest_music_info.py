#!/usr/bin/env python3
# =============================================================================
#  SoundLab Analytics — Bloc 6 Big Data
#  Tâche 7 : pipeline d'ingestion du Million Song Dataset (métadonnées)   [v2]
#
#  Source      s3://<raw>/msd/music_info.csv        (50 683 lignes, 21 colonnes)
#  Cible       s3://<curated>/music_info/           (Parquet + Snappy)
#  Catalogue   table Glue <db>.music_info           (interrogeable via Athena)
#
#  ---------------------------------------------------------------------------
#  Différences avec la v1, et pourquoi
#
#  1. PLUS DE METASTORE HIVE. La v1 passait par `enableHiveSupport()` et
#     `saveAsTable()`, ce qui fait transiter Spark par le pont
#     AWSGlueDataCatalogHiveClientFactory. Ce pont vérifie puis tente de créer
#     la base `default` au démarrage, indépendamment de la base réellement
#     visée — trois points de défaillance avant la première ligne de données.
#     La v2 écrit du Parquet nu, puis déclare la table directement via l'API
#     Glue. Moins magique, entièrement déterministe, et le schéma de la table
#     est explicite plutôt que déduit.
#
#  2. CLIENTS AWS RÉGIONAUX ET IMPATIENTS. La v1 instanciait boto3 sans
#     région : l'endpoint global ne résout pas pour eu-north-1, et botocore a
#     retenté pendant 40 minutes avant d'abandonner. La v2 fixe la région et
#     impose un timeout de connexion de 5 s avec 3 tentatives maximum. Un job
#     qui doit échouer doit échouer vite : le temps de calcul est facturé.
#  ---------------------------------------------------------------------------
# =============================================================================

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

# -----------------------------------------------------------------------------
# Schéma explicite
#
# Pourquoi ne PAS utiliser inferSchema : Spark devrait lire l'intégralité du
# fichier une première fois rien que pour deviner les types, ce qui double le
# coût d'ingestion. Surtout, l'inférence est instable — une colonne entière
# devient un double si une seule ligne est mal formée, et le pipeline change
# silencieusement de comportement d'une exécution à l'autre. Un schéma déclaré
# est à la fois plus rapide et contractuel.
# -----------------------------------------------------------------------------
SCHEMA = StructType([
    StructField("track_id",            StringType(),  nullable=False),
    StructField("name",                StringType(),  nullable=True),
    StructField("artist",              StringType(),  nullable=True),
    StructField("spotify_preview_url", StringType(),  nullable=True),
    StructField("spotify_id",          StringType(),  nullable=True),
    StructField("tags",                StringType(),  nullable=True),
    StructField("genre",               StringType(),  nullable=True),
    StructField("year",                IntegerType(), nullable=True),
    StructField("duration_ms",         IntegerType(), nullable=True),
    # --- les 13 caractéristiques audio Spotify utilisées par le modèle ---
    StructField("danceability",        DoubleType(),  nullable=True),
    StructField("energy",              DoubleType(),  nullable=True),
    StructField("key",                 IntegerType(), nullable=True),
    StructField("loudness",            DoubleType(),  nullable=True),
    StructField("mode",                IntegerType(), nullable=True),
    StructField("speechiness",         DoubleType(),  nullable=True),
    StructField("acousticness",        DoubleType(),  nullable=True),
    StructField("instrumentalness",    DoubleType(),  nullable=True),
    StructField("liveness",            DoubleType(),  nullable=True),
    StructField("valence",             DoubleType(),  nullable=True),
    StructField("tempo",               DoubleType(),  nullable=True),
    StructField("time_signature",      IntegerType(), nullable=True),
    # Colonne technique : reçoit la ligne brute quand elle est illisible
    StructField("_corrupt_record",     StringType(),  nullable=True),
])

AUDIO_FEATURES = [
    "danceability", "energy", "key", "loudness", "mode", "speechiness",
    "acousticness", "instrumentalness", "liveness", "valence", "tempo",
    "time_signature", "duration_ms",
]

# Bornes de validité issues de la documentation de l'API Spotify.
# Contrôle de vraisemblance, pas nettoyage agressif.
BORNES = {
    "danceability": (0.0, 1.0), "energy": (0.0, 1.0), "speechiness": (0.0, 1.0),
    "acousticness": (0.0, 1.0), "instrumentalness": (0.0, 1.0),
    "liveness": (0.0, 1.0), "valence": (0.0, 1.0),
    "loudness": (-60.0, 5.0), "tempo": (0.0, 250.0),
    "key": (-1, 11), "mode": (0, 1), "time_signature": (0, 7),
}

# Correspondance types Spark -> types Glue/Hive, pour déclarer la table
TYPES_GLUE = {
    "StringType()": "string", "StringType": "string",
    "IntegerType()": "int", "IntegerType": "int",
    "DoubleType()": "double", "DoubleType": "double",
    "TimestampType()": "timestamp", "TimestampType": "timestamp",
    "LongType()": "bigint", "LongType": "bigint",
}

# Clients AWS impatients : 5 s pour se connecter, 3 tentatives, pas plus.
CONFIG_AWS = Config(
    connect_timeout=5, read_timeout=30, retries={"max_attempts": 3, "mode": "standard"}
)


def journal(msg: str) -> None:
    """Horodatage explicite : les logs EMR sont lus a posteriori dans S3."""
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def decouper_s3(uri: str):
    """s3://bucket/prefixe/ -> ('bucket', 'prefixe')"""
    reste = uri.replace("s3://", "").rstrip("/")
    bucket, _, prefixe = reste.partition("/")
    return bucket, prefixe


def declarer_table_glue(glue, base: str, table: str, emplacement: str, schema) -> None:
    """
    Déclare (ou met à jour) la table dans le catalogue Glue via l'API, sans
    passer par Hive. Le schéma est transmis explicitement : c'est ce qui rend
    la table immédiatement interrogeable par Athena, sans crawler ni MSCK.
    """
    colonnes = [
        {"Name": champ.name, "Type": TYPES_GLUE.get(str(champ.dataType), "string")}
        for champ in schema.fields
    ]

    definition = {
        "Name": table,
        "Description": "Métadonnées et caractéristiques audio du Million Song Dataset",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "EXTERNAL": "TRUE",
            "projet": "SoundLab",
            "tache": "07",
        },
        "StorageDescriptor": {
            "Columns": colonnes,
            "Location": emplacement,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary":
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "Compressed": True,
        },
    }

    try:
        glue.create_table(DatabaseName=base, TableInput=definition)
        journal(f"Table Glue {base}.{table} créée")
    except ClientError as err:
        if err.response["Error"]["Code"] == "AlreadyExistsException":
            glue.update_table(DatabaseName=base, TableInput=definition)
            journal(f"Table Glue {base}.{table} mise à jour")
        else:
            raise


def main() -> int:
    p = argparse.ArgumentParser(description="Ingestion MSD — métadonnées")
    p.add_argument("--source",  required=True, help="s3://<raw>/msd/music_info.csv")
    p.add_argument("--cible",   required=True, help="s3://<curated>/music_info/")
    p.add_argument("--rapport", required=True, help="s3://<logs>/qualite/tache07/")
    p.add_argument("--glue-db", required=True)
    p.add_argument("--table",   default="music_info")
    p.add_argument("--region",  default=os.environ.get("AWS_REGION", "eu-north-1"),
                   help="Région AWS — indispensable, l'endpoint S3 global ne "
                        "résout pas pour les régions récentes comme eu-north-1")
    args = p.parse_args()

    journal(f"Région AWS : {args.region}")
    s3 = boto3.client("s3", region_name=args.region, config=CONFIG_AWS)
    glue = boto3.client("glue", region_name=args.region, config=CONFIG_AWS)

    spark = (
        SparkSession.builder
        .appName("soundlab-tache07-ingestion-music-info")
        # Snappy : moins compact que gzip mais *splittable*, donc lisible en
        # parallèle par plusieurs exécuteurs. Bon compromis pour un entrepôt.
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Laisse Spark fusionner les partitions trop petites à l'exécution.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    journal(f"Spark {spark.version} démarré")

    metriques: dict = {
        "tache": "07_ingestion_music_info",
        "horodatage_utc": datetime.now(timezone.utc).isoformat(),
        "source": args.source,
        "cible": args.cible,
    }

    # -------------------------------------------------------------------------
    # 1. Lecture
    #    mode=PERMISSIVE : une ligne illisible ne fait pas échouer le job, elle
    #    atterrit dans _corrupt_record. On la compte, on l'archive, on continue.
    # -------------------------------------------------------------------------
    journal("Lecture du CSV source")
    brut = (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')          # les champs `tags` contiennent des virgules
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(SCHEMA)
        .csv(args.source)
        .cache()
    )

    n_brut = brut.count()
    n_corrompu = brut.filter(F.col("_corrupt_record").isNotNull()).count()
    metriques["lignes_lues"] = n_brut
    metriques["lignes_corrompues"] = n_corrompu
    journal(f"{n_brut:,} lignes lues, dont {n_corrompu} illisibles")

    if n_corrompu:
        # Un pipeline qui jette silencieusement des données n'est pas auditable.
        (brut.filter(F.col("_corrupt_record").isNotNull())
             .select("_corrupt_record")
             .write.mode("overwrite").text(args.rapport.rstrip("/") + "/rejets/"))
        journal("Lignes rejetées archivées")

    df = brut.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    n_valides = df.count()

    # -------------------------------------------------------------------------
    # 2. Contrôles de qualité, avant toute transformation
    # -------------------------------------------------------------------------
    journal("Profilage qualité")

    metriques["taux_valeurs_nulles_pct"] = df.select([
        F.round(F.avg(F.col(c).isNull().cast("double")) * 100, 3).alias(c)
        for c in df.columns
    ]).first().asDict()

    n_distincts = df.select("track_id").distinct().count()
    metriques["track_id_distincts"] = n_distincts
    metriques["doublons_track_id"] = n_valides - n_distincts
    journal(f"{n_distincts:,} track_id distincts ({n_valides - n_distincts} doublons)")

    hors_bornes = {}
    for col, (mini, maxi) in BORNES.items():
        n = df.filter((F.col(col) < mini) | (F.col(col) > maxi)).count()
        if n:
            hors_bornes[col] = n
    metriques["valeurs_hors_bornes"] = hors_bornes
    if hors_bornes:
        journal(f"Valeurs hors bornes : {hors_bornes}")

    # -------------------------------------------------------------------------
    # 3. Transformation
    #    Trois opérations, toutes justifiables :
    #      a) déduplication sur la clé métier track_id
    #      b) rejet des lignes sans identifiant ou sans caractéristique audio
    #      c) normalisation légère des chaînes
    #    Aucune imputation ici : elle relève du feature engineering (tâche 10).
    #    La couche curated doit rester fidèle à la source.
    # -------------------------------------------------------------------------
    journal("Transformation")

    df = (
        df
        .dropDuplicates(["track_id"])
        .filter(F.col("track_id").isNotNull() & (F.length(F.trim("track_id")) > 0))
        # Une piste sans aucune caractéristique audio est inexploitable pour le
        # modèle : on la sort dès l'ingestion plutôt que de la traîner.
        .filter(F.coalesce(*[F.col(c) for c in AUDIO_FEATURES]).isNotNull())
        .withColumn("name",   F.trim(F.col("name")))
        .withColumn("artist", F.trim(F.col("artist")))
        .withColumn("genre",  F.lower(F.trim(F.col("genre"))))
        # `year` à 0 signifie « inconnu » dans le MSD : on l'explicite en NULL
        # pour que les agrégations ne le comptent pas comme une année réelle.
        .withColumn("year", F.when(F.col("year") > 1900, F.col("year")))
        .withColumn("duration_min", F.round(F.col("duration_ms") / 60000.0, 3))
        .withColumn("nb_tags",
                    F.when(F.col("tags").isNull(), F.lit(0))
                     .otherwise(F.size(F.split(F.col("tags"), ","))))
        .withColumn("date_ingestion", F.current_timestamp())
    )

    n_final = df.count()
    metriques["lignes_ecrites"] = n_final
    metriques["lignes_ecartees"] = n_valides - n_final
    journal(f"{n_final:,} lignes retenues ({n_valides - n_final} écartées)")

    # -------------------------------------------------------------------------
    # 4. Écriture
    #    Pas de partitionnement : 50 000 lignes ne justifient pas un découpage
    #    par genre ou par année — on créerait des centaines de fichiers de
    #    quelques kilo-octets, et chaque lecture ultérieure paierait le coût
    #    d'ouvrir tous ces objets S3 (le « small files problem »). On vise
    #    2 fichiers d'environ 5 Mo. L'arbitrage sera inverse en tâche 8.
    # -------------------------------------------------------------------------
    journal(f"Écriture Parquet vers {args.cible}")
    df.coalesce(2).write.mode("overwrite").parquet(args.cible)
    journal("Écriture terminée")

    # -------------------------------------------------------------------------
    # 5. Déclaration au catalogue Glue, via l'API et non via Hive
    # -------------------------------------------------------------------------
    declarer_table_glue(glue, args.glue_db, args.table, args.cible.rstrip("/"), df.schema)

    # -------------------------------------------------------------------------
    # 6. Rapport de qualité — livrable « documentation » du brief
    # -------------------------------------------------------------------------
    metriques["statut"] = "SUCCES"
    bucket, prefixe = decouper_s3(args.rapport)
    cle = f"{prefixe}/rapport_qualite_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.json"
    s3.put_object(
        Bucket=bucket, Key=cle,
        Body=json.dumps(metriques, indent=2, ensure_ascii=False).encode(),
        ContentType="application/json",
    )
    journal(f"Rapport qualité : s3://{bucket}/{cle}")

    journal("Tâche 7 terminée avec succès")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
