#!/usr/bin/env python3
# =============================================================================
#  SoundLab Analytics — Bloc 6 Big Data
#  Tâche 7 : pipeline d'ingestion du Million Song Dataset (métadonnées)
#
#  Source      s3://<raw>/msd/music_info.csv        (50 683 lignes, 21 colonnes)
#  Cible       s3://<curated>/music_info/           (Parquet + Snappy)
#  Catalogue   table Glue <db>.music_info           (interrogeable via Athena)
#
#  Exécuté sur EMR Serverless. Aucun secret, aucun chemin en dur : tout arrive
#  par arguments, ce qui rend le job rejouable sur n'importe quel environnement.
# =============================================================================

import argparse
import json
import sys
from datetime import datetime, timezone

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

# -----------------------------------------------------------------------------
# Schéma explicite
#
# Pourquoi ne PAS utiliser inferSchema : Spark devrait lire l'intégralité du
# fichier une première fois rien que pour deviner les types, ce qui double le
# coût d'ingestion. Surtout, l'inférence est instable — une colonne entière
# devient un double si une seule ligne est mal formée, et le pipeline change
# silencieusement de comportement d'une exécution à l'autre. Un schéma déclaré
# est à la fois plus rapide et contractuel : si la source change, le job le
# signale au lieu de s'adapter en douce.
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
# Servent de contrôle de vraisemblance, pas de nettoyage agressif.
BORNES = {
    "danceability": (0.0, 1.0), "energy": (0.0, 1.0), "speechiness": (0.0, 1.0),
    "acousticness": (0.0, 1.0), "instrumentalness": (0.0, 1.0),
    "liveness": (0.0, 1.0), "valence": (0.0, 1.0),
    "loudness": (-60.0, 5.0), "tempo": (0.0, 250.0),
    "key": (-1, 11), "mode": (0, 1), "time_signature": (0, 7),
}


def journal(msg: str) -> None:
    """Horodatage explicite : les logs EMR sont lus a posteriori dans S3."""
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def main() -> int:
    p = argparse.ArgumentParser(description="Ingestion MSD — métadonnées")
    p.add_argument("--source",   required=True, help="s3://<raw>/msd/music_info.csv")
    p.add_argument("--cible",    required=True, help="s3://<curated>/music_info/")
    p.add_argument("--rapport",  required=True, help="s3://<logs>/qualite/tache07/")
    p.add_argument("--glue-db",  required=True)
    p.add_argument("--table",    default="music_info")
    args = p.parse_args()

    spark = (
        SparkSession.builder
        .appName("soundlab-tache07-ingestion-music-info")
        # Le catalogue Glue tient lieu de metastore Hive : la table écrite ici
        # devient immédiatement interrogeable depuis Athena, sans crawler.
        .enableHiveSupport()
        # Écriture Parquet : Snappy est le bon compromis pour Spark — moins
        # compressé que gzip mais *splittable*, donc parallélisable en lecture.
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Laisse Spark fusionner les partitions trop petites à la volée.
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
    #    atterrit dans _corrupt_record. On la compte, on la met de côté, et on
    #    continue — c'est le comportement attendu d'un pipeline de production.
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
        # On conserve les rejets : un pipeline qui jette silencieusement des
        # données n'est pas auditable.
        (brut.filter(F.col("_corrupt_record").isNotNull())
             .select("_corrupt_record")
             .write.mode("overwrite").text(args.rapport.rstrip("/") + "/rejets/"))
        journal("Lignes rejetées archivées")

    df = brut.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    # -------------------------------------------------------------------------
    # 2. Contrôles de qualité (avant transformation)
    # -------------------------------------------------------------------------
    journal("Profilage qualité")

    taux_nuls = df.select([
        F.round(F.avg(F.col(c).isNull().cast("double")) * 100, 3).alias(c)
        for c in df.columns
    ]).first().asDict()
    metriques["taux_valeurs_nulles_pct"] = taux_nuls

    n_distincts = df.select("track_id").distinct().count()
    metriques["track_id_distincts"] = n_distincts
    metriques["doublons_track_id"] = df.count() - n_distincts
    journal(f"{n_distincts:,} track_id distincts "
            f"({df.count() - n_distincts} doublons)")

    hors_bornes = {}
    for col, (mini, maxi) in BORNES.items():
        n = df.filter((F.col(col) < mini) | (F.col(col) > maxi)).count()
        if n:
            hors_bornes[col] = n
    metriques["valeurs_hors_bornes"] = hors_bornes
    if hors_bornes:
        journal(f"Valeurs hors bornes détectées : {hors_bornes}")

    # -------------------------------------------------------------------------
    # 3. Transformation
    #    Trois opérations seulement, toutes justifiables :
    #      a) déduplication sur la clé métier track_id
    #      b) rejet des lignes sans identifiant ou sans caractéristique audio
    #      c) normalisation légère des chaînes (espaces, casse du genre)
    #    Aucune imputation ici : elle relève du feature engineering (tâche 10),
    #    pas de l'ingestion. La couche curated doit rester fidèle à la source.
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
        # Colonnes dérivées utiles dès maintenant
        .withColumn("duration_min", F.round(F.col("duration_ms") / 60000.0, 3))
        .withColumn("nb_tags",
                    F.when(F.col("tags").isNull(), F.lit(0))
                     .otherwise(F.size(F.split(F.col("tags"), ","))))
        .withColumn("date_ingestion", F.current_timestamp())
    )

    n_final = df.count()
    metriques["lignes_ecrites"] = n_final
    metriques["lignes_ecartees"] = n_brut - n_corrompu - n_final
    journal(f"{n_final:,} lignes retenues "
            f"({n_brut - n_corrompu - n_final} écartées)")

    # -------------------------------------------------------------------------
    # 4. Écriture
    #    Pas de partitionnement : 50 000 lignes ne justifient pas de découper
    #    par genre ou par année — on créerait des centaines de fichiers de
    #    quelques kilo-octets, ce qui dégrade les lectures ultérieures (le
    #    « small files problem »). On vise plutôt 2 fichiers de ~5 Mo.
    # -------------------------------------------------------------------------
    journal(f"Écriture Parquet vers {args.cible}")
    (
        df.coalesce(2)
          .write
          .mode("overwrite")
          .option("path", args.cible)
          .format("parquet")
          .saveAsTable(f"{args.glue_db}.{args.table}")
    )
    journal(f"Table Glue {args.glue_db}.{args.table} enregistrée")

    # -------------------------------------------------------------------------
    # 5. Rapport de qualité — livrable « documentation » du brief
    # -------------------------------------------------------------------------
    metriques["statut"] = "SUCCES"
    bucket, _, prefixe = args.rapport.replace("s3://", "").partition("/")
    cle = f"{prefixe.rstrip('/')}/rapport_qualite_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.json"
    boto3.client("s3").put_object(
        Bucket=bucket, Key=cle,
        Body=json.dumps(metriques, indent=2, ensure_ascii=False).encode(),
        ContentType="application/json",
    )
    journal(f"Rapport qualité : s3://{bucket}/{cle}")

    journal("Tâche 7 terminée")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
