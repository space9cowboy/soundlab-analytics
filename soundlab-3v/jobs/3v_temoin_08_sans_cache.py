#!/usr/bin/env python3
# =============================================================================
#  SoundLab Analytics — Bloc 6 Big Data
#  Tâche 8 : ingestion de l'historique d'écoute avec pseudonymisation
#
#  Source   s3://<raw>/msd/user_listening_history.csv   (9 711 301 lignes)
#  Cible    s3://<curated>/listening_history/           (Parquet + Snappy)
#  Table    <glue_db>.listening_history
#
#  ---------------------------------------------------------------------------
#  CE QUI CHANGE PAR RAPPORT À LA TÂCHE 7, ET POURQUOI
#
#  1. NOMBRE DE FICHIERS DE SORTIE. La tâche 7 écrivait 2 fichiers via
#     coalesce(2) : sur 50 000 lignes, découper davantage aurait produit des
#     objets minuscules coûteux à lister. Ici, 2 fichiers signifieraient que
#     seules 2 tâches Spark peuvent lire la table en parallèle, quel que soit
#     le nombre d'exécuteurs disponibles. On vise donc ~12 fichiers de 10 à
#     20 Mo : assez pour paralléliser, assez gros pour ne pas payer le coût
#     d'ouverture de chaque objet S3.
#
#  2. CONTRÔLES QUALITÉ EN UNE SEULE PASSE. La tâche 7 enchaînait dix-sept
#     actions Spark distinctes (un count() par contrôle). Indolore sur 50 000
#     lignes en cache, ruineux sur 9,7 millions : chaque action relance un
#     parcours complet. Tous les contrôles sont ici repliés dans une unique
#     expression agg(), donc un seul parcours.
#
#  3. PSEUDONYMISATION. Nouveau maillon, absent de la tâche 7 parce que les
#     métadonnées de pistes ne contiennent aucune donnée personnelle. Ici,
#     `user_id` identifie une personne : il est remplacé par un SHA-256 salé
#     avant toute écriture, et l'identifiant en clair ne sort jamais du job.
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

# Clients AWS impatients : voir l'incident de la tâche 7 (41 min facturées
# pour une erreur de connexion). Un job qui doit échouer doit échouer vite.
CONFIG_AWS = Config(
    connect_timeout=5, read_timeout=30, retries={"max_attempts": 3, "mode": "standard"}
)

COLONNES_ATTENDUES = {"user_id", "track_id", "playcount"}

TYPES_GLUE = {
    "StringType()": "string", "StringType": "string",
    "IntegerType()": "int", "IntegerType": "int",
    "LongType()": "bigint", "LongType": "bigint",
    "DoubleType()": "double", "DoubleType": "double",
    "TimestampType()": "timestamp", "TimestampType": "timestamp",
}


def journal(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def decouper_s3(uri: str):
    reste = uri.replace("s3://", "").rstrip("/")
    bucket, _, prefixe = reste.partition("/")
    return bucket, prefixe


def mesurer_taille_s3(s3, uri: str) -> dict:
    """
    Somme la taille des objets écrits sous un préfixe.

    Mesurer la sortie fait partie du job : sans cette valeur dans le rapport,
    l'effet d'un choix d'encodage ne se compare pas d'une exécution à l'autre.
    """
    bucket, prefixe = decouper_s3(uri)
    total, fichiers = 0, 0
    paginateur = s3.get_paginator("list_objects_v2")
    for page in paginateur.paginate(Bucket=bucket, Prefix=prefixe + "/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                total += obj["Size"]
                fichiers += 1
    return {
        "fichiers": fichiers,
        "octets": total,
        "mio": round(total / 1024 / 1024, 1),
        "octets_par_ligne": None,      # complété par l'appelant
    }


def lire_sel(secrets, arn: str) -> str:
    """
    Récupère le sel de pseudonymisation depuis Secrets Manager.

    Le sel n'est ni un argument du job, ni une variable d'environnement, ni
    une valeur en dur : il est lu à l'exécution par le pilote, avec un rôle
    IAM autorisé sur ce seul secret. C'est ce qui permet d'affirmer, dans le
    DPIA, que la table pseudonymisée ne peut pas être ré-identifiée par
    quelqu'un qui n'aurait accès qu'aux données.
    """
    reponse = secrets.get_secret_value(SecretId=arn)
    sel = json.loads(reponse["SecretString"])["salt"]
    if not sel or len(sel) < 32:
        raise ValueError("Sel absent ou trop court — pseudonymisation non fiable")
    return sel


def declarer_table_glue(glue, base: str, table: str, emplacement: str, schema) -> None:
    """Déclaration explicite via l'API Glue — pas de metastore Hive (cf. tâche 7)."""
    colonnes = [
        {"Name": c.name, "Type": TYPES_GLUE.get(str(c.dataType), "string")}
        for c in schema.fields
    ]
    definition = {
        "Name": table,
        "Description": "Historique d'écoute pseudonymisé (SHA-256 salé) — Taste Profile",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet", "EXTERNAL": "TRUE",
            "projet": "SoundLab", "tache": "08",
            "donnees_personnelles": "pseudonymisees",
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
    p = argparse.ArgumentParser(description="Ingestion Taste Profile — pseudonymisée")
    p.add_argument("--source",     required=True)
    p.add_argument("--cible",      required=True)
    p.add_argument("--rapport",    required=True)
    p.add_argument("--secret-arn", required=True)
    p.add_argument("--glue-db",    required=True)
    p.add_argument("--table",      default="listening_history")
    p.add_argument("--fichiers",   type=int, default=12,
                   help="Nombre de fichiers Parquet en sortie")
    p.add_argument("--bits-hachage", type=int, default=128,
                   help="Longueur retenue du SHA-256, en bits (multiple de 4, "
                        "max 256). 128 bits = 32 caractères hexadécimaux.")
    p.add_argument("--region",     default=os.environ.get("AWS_REGION", "eu-north-1"))
    args = p.parse_args()

    if args.bits_hachage % 4 or not 64 <= args.bits_hachage <= 256:
        raise ValueError("--bits-hachage doit être un multiple de 4, entre 64 et 256")
    n_hex = args.bits_hachage // 4

    journal(f"Région AWS : {args.region}")
    s3      = boto3.client("s3", region_name=args.region, config=CONFIG_AWS)
    glue    = boto3.client("glue", region_name=args.region, config=CONFIG_AWS)
    secrets = boto3.client("secretsmanager", region_name=args.region, config=CONFIG_AWS)

    # -------------------------------------------------------------------------
    # 0. Sel de pseudonymisation — avant tout traitement.
    #    Si le secret est inaccessible, le job doit s'arrêter immédiatement :
    #    mieux vaut ne rien produire que produire des données non pseudonymisées.
    # -------------------------------------------------------------------------
    journal("Lecture du sel dans Secrets Manager")
    sel = lire_sel(secrets, args.secret_arn)
    journal(f"Sel obtenu ({len(sel)} caractères) — valeur jamais journalisée")

    spark = (
        SparkSession.builder
        .appName("soundlab-tache08-ingestion-listening-history")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Évite que Spark écrive des fichiers de contrôle inutiles sur S3
        .config("spark.sql.sources.commitProtocolClass",
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    journal(f"Spark {spark.version} démarré")

    metriques = {
        "tache": "08_ingestion_listening_history",
        "horodatage_utc": datetime.now(timezone.utc).isoformat(),
        "source": args.source,
        "cible": args.cible,
    }

    # -------------------------------------------------------------------------
    # 1. Lecture
    #    Contrairement à la tâche 7, le schéma n'est pas appliqué par position
    #    mais par NOM. Avec un schéma positionnel, une inversion de deux
    #    colonnes dans la source produirait des données fausses sans la
    #    moindre erreur — ici, un jeu de colonnes inattendu arrête le job.
    #    Tout est lu en texte puis converti explicitement : la lecture de
    #    l'en-tête ne coûte qu'un accès métadonnées, contrairement à
    #    inferSchema qui parcourrait les 575 Mo.
    # -------------------------------------------------------------------------
    journal("Lecture du CSV source")
    brut = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .csv(args.source)
    )

    colonnes = set(brut.columns)
    if not COLONNES_ATTENDUES.issubset(colonnes):
        raise ValueError(
            f"Colonnes inattendues. Attendu au moins {sorted(COLONNES_ATTENDUES)}, "
            f"trouvé {sorted(colonnes)}"
        )
    journal(f"Colonnes validées : {sorted(colonnes)}")

    df = brut.select(
        F.col("user_id").cast("string").alias("user_id"),
        F.col("track_id").cast("string").alias("track_id"),
        F.col("playcount").cast("int").alias("playcount"),
    )

    # -------------------------------------------------------------------------
    # 2. Contrôles qualité — UN SEUL PARCOURS
    #
    #    Toute la campagne de contrôles tient dans une expression agg().
    #    Sur 9,7 M lignes, la différence avec l'approche de la tâche 7
    #    (un count() par contrôle) est de l'ordre d'un facteur dix.
    #
    #    Les deux countDistinct sont exacts et non approximés : ils servent au
    #    contrôle anti-collision du § 4, qui perdrait tout sens avec une
    #    estimation. Pour un simple ordre de grandeur, approx_count_distinct
    #    (HyperLogLog, ~1 % d'erreur) éviterait le shuffle et serait préférable.
    # -------------------------------------------------------------------------
    journal("Profilage qualité (passe unique)")
    q = df.agg(
        F.count("*").alias("lignes"),
        F.sum(F.col("user_id").isNull().cast("long")).alias("nuls_user"),
        F.sum(F.col("track_id").isNull().cast("long")).alias("nuls_track"),
        F.sum(F.col("playcount").isNull().cast("long")).alias("nuls_playcount"),
        F.sum((F.col("playcount") <= 0).cast("long")).alias("playcount_non_positif"),
        F.min("playcount").alias("playcount_min"),
        F.max("playcount").alias("playcount_max"),
        F.round(F.avg("playcount"), 3).alias("playcount_moyen"),
        F.countDistinct("user_id").alias("utilisateurs_distincts"),
        F.countDistinct("track_id").alias("pistes_distinctes"),
    ).first().asDict()

    metriques["qualite_source"] = q
    journal(f"{q['lignes']:,} lignes · {q['utilisateurs_distincts']:,} utilisateurs · "
            f"{q['pistes_distinctes']:,} pistes")
    journal(f"playcount : min {q['playcount_min']}, max {q['playcount_max']}, "
            f"moyenne {q['playcount_moyen']}")

    if q["nuls_user"] or q["nuls_track"] or q["nuls_playcount"]:
        journal(f"Valeurs nulles détectées : user={q['nuls_user']}, "
                f"track={q['nuls_track']}, playcount={q['nuls_playcount']}")

    # -------------------------------------------------------------------------
    # 3. Pseudonymisation
    #
    #    user_id_hash = SHA-256(sel || user_id)
    #
    #    Pourquoi un sel : un identifiant utilisateur est court et tiré d'un
    #    espace restreint. Un SHA-256 non salé se casse par force brute ou par
    #    table arc-en-ciel en quelques minutes — le hachage seul ne suffirait
    #    pas à parler de pseudonymisation au sens du RGPD. Le sel, conservé
    #    dans Secrets Manager et jamais écrit à côté des données, rend
    #    l'attaque impraticable pour qui n'a accès qu'à la table.
    #
    #    Le hachage est déterministe : le même utilisateur reçoit toujours le
    #    même jeton, ce qui préserve la possibilité de compter des auditeurs
    #    uniques par piste — la variable `unique_listeners`, décisive pour le
    #    modèle. C'est le compromis exact de la pseudonymisation : on perd
    #    l'identité, on garde la structure.
    #
    #    Risque résiduel assumé, à consigner dans le DPIA : le sel apparaît
    #    en clair dans le plan physique Spark, susceptible d'être journalisé.
    #    Le bucket de logs est donc fermé (aucun accès public, TLS obligatoire,
    #    accès restreint au rôle d'exécution).
    #
    #    TRONCATURE — décision de stockage, pas de sécurité.
    #    Un SHA-256 complet occupe 64 caractères hexadécimaux. Comme un
    #    hachage est indistinguable du hasard, il ne se compresse pas : ni
    #    Snappy, qui cherche des motifs répétés, ni le dictionnaire Parquet,
    #    inutile face à ~1 M de valeurs distinctes. Cette seule colonne pèse
    #    donc plus lourd que le CSV source tout entier.
    #
    #    On retient les 128 premiers bits. Pour N utilisateurs, la probabilité
    #    de collision vaut approximativement N² / 2^(bits+1), soit ici
    #    (9,6e5)² / 2^129 ≈ 1,4e-27 — hors d'atteinte. Et si l'improbable
    #    survenait, le contrôle du § 4 arrêterait le job.
    #
    #    Ce que la troncature ne change PAS : la résistance à la
    #    ré-identification, qui repose entièrement sur le secret du sel, pas
    #    sur la longueur du condensat.
    # -------------------------------------------------------------------------
    journal(f"Pseudonymisation SHA-256 salée, tronquée à {args.bits_hachage} bits "
            f"({n_hex} caractères hexadécimaux)")
    pseudo = (
        df
        .withColumn(
            "user_id_hash",
            F.substring(F.sha2(F.concat(F.lit(sel), F.col("user_id")), 256), 1, n_hex),
        )
        .withColumn("date_ingestion", F.current_timestamp())
        .drop("user_id")          # l'identifiant en clair ne va pas plus loin
    )

    assert "user_id" not in pseudo.columns, "user_id en clair encore présent"

    # -------------------------------------------------------------------------
    # 4. Contrôle anti-collision
    #    Le nombre de jetons distincts doit égaler le nombre d'utilisateurs
    #    distincts de la source. Une différence signalerait une collision de
    #    hachage — c'est-à-dire deux personnes fusionnées en une seule dans
    #    toutes les analyses en aval.
    # -------------------------------------------------------------------------
    journal("Contrôle d'intégrité de la pseudonymisation")
    n_jetons = pseudo.select("user_id_hash").distinct().count()
    metriques["jetons_distincts"] = n_jetons
    metriques["collisions"] = q["utilisateurs_distincts"] - n_jetons

    if n_jetons != q["utilisateurs_distincts"]:
        raise ValueError(
            f"Collision de hachage : {q['utilisateurs_distincts']:,} utilisateurs "
            f"pour {n_jetons:,} jetons distincts"
        )
    journal(f"{n_jetons:,} jetons distincts — bijection préservée, 0 collision")

    # -------------------------------------------------------------------------
    # 5. Écriture
    #
    #    repartition(N) et non coalesce : repartition redistribue réellement
    #    les lignes entre N partitions de taille homogène, au prix d'un
    #    shuffle. coalesce se contenterait de fusionner les partitions
    #    existantes, sans rééquilibrage, ce qui produirait des fichiers de
    #    tailles très inégales — et donc des tâches de lecture inégales.
    #
    #    Pourquoi 12 : une partition Spark est écrite par une tâche, et une
    #    tâche produit un fichier. Le facteur limitant est donc l'ÉCRITURE —
    #    avec repartition(1), les 9,7 M lignes passeraient par un seul cœur.
    #    12 correspond au parallélisme disponible (3 exécuteurs x 4 cœurs).
    #    À la lecture, la contrainte est plus faible qu'il n'y paraît :
    #    Parquet est splittable, Spark découpe un même fichier aux frontières
    #    des row groups selon spark.sql.files.maxPartitionBytes (128 Mo).
    #    La borne haute reste le « small files problem » : 200 partitions de
    #    shuffle par défaut donneraient 200 fichiers de 3 Mo, dont le coût
    #    d'ouverture et d'ordonnancement dépasserait le travail utile.
    #
    #    Pourquoi PAS de partitionBy(colonne) : le partitionnement Hive en
    #    répertoires n'a d'intérêt que si les requêtes filtrent sur la clé de
    #    partition. Or l'usage aval est une jointure avec music_info sur
    #    track_id, qui lit la table entière : aucun élagage possible. Ajouter
    #    des répertoires ne ferait que multiplier les objets S3 pour rien.
    #    Le bucketing, lui, accélérerait cette jointure, mais il exige un
    #    metastore Hive — abandonné en tâche 7 pour cause d'instabilité.
    # -------------------------------------------------------------------------
    journal(f"Écriture Parquet vers {args.cible} ({args.fichiers} fichiers)")
    sortie = pseudo.repartition(args.fichiers)
    sortie.write.mode("overwrite").parquet(args.cible)
    journal("Écriture terminée")

    metriques["fichiers_ecrits"] = args.fichiers
    metriques["lignes_ecrites"] = q["lignes"]

    # Mesure de la sortie : c'est cette valeur qui rend comparables deux
    # exécutions avec des longueurs de condensat différentes.
    taille = mesurer_taille_s3(s3, args.cible)
    taille["octets_par_ligne"] = round(taille["octets"] / q["lignes"], 2)
    metriques["taille_sortie"] = taille
    journal(f"Sortie : {taille['fichiers']} fichiers, {taille['mio']} Mio "
            f"({taille['octets_par_ligne']} octets/ligne)")

    # -------------------------------------------------------------------------
    # 6. Catalogue
    # -------------------------------------------------------------------------
    declarer_table_glue(glue, args.glue_db, args.table,
                        args.cible.rstrip("/"), sortie.schema)

    # -------------------------------------------------------------------------
    # 7. Rapport de qualité
    # -------------------------------------------------------------------------
    metriques["statut"] = "SUCCES"
    metriques["pseudonymisation"] = {
        "algorithme": "SHA-256 salé",
        "bits_retenus": args.bits_hachage,
        "caracteres_hexadecimaux": n_hex,
        "probabilite_collision_theorique": f"~{q['utilisateurs_distincts'] ** 2 / 2 ** (args.bits_hachage + 1):.1e}",
        "sel": "Secrets Manager, jamais journalisé",
        "colonne_source_supprimee": "user_id",
        "colonne_produite": "user_id_hash",
        "collisions_detectees": 0,
    }
    bucket, prefixe = decouper_s3(args.rapport)
    cle = f"{prefixe}/rapport_qualite_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.json"
    s3.put_object(
        Bucket=bucket, Key=cle,
        Body=json.dumps(metriques, indent=2, ensure_ascii=False, default=str).encode(),
        ContentType="application/json",
    )
    journal(f"Rapport qualité : s3://{bucket}/{cle}")

    journal("Tâche 8 terminée avec succès")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
