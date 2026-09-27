#!/usr/bin/env python3
# =============================================================================
#  SoundLab Analytics — Bloc 6 Big Data
#  Tâches 10-11 : feature engineering et préparation du jeu d'entraînement
#
#  Entrées   s3://<curated>/music_info/          50 683 pistes
#            s3://<curated>/listening_history/   9 711 301 écoutes pseudonymisées
#
#  Sorties   s3://<curated>/track_engagement/          agrégat par piste
#            s3://<curated>/songs_features_labeled/    table d'entraînement
#            s3://<logs>/qualite/tache10/              diagnostics + statistiques
#
#  ---------------------------------------------------------------------------
#  LE PROBLÈME QUE CE JOB DOIT RENDRE VISIBLE : LA FUITE DE CIBLE
#
#  La cible se définit ainsi :        is_hit = 1  si  total_plays >= P75
#  Or  total_plays = SUM(playcount)  et  unique_listeners = COUNT(*)
#  sont calculés sur EXACTEMENT LES MÊMES LIGNES de listening_history.
#
#  Le playcount moyen valant 2,631, on a  total_plays ~ 2,631 x unique_listeners.
#  Un modèle nourri de `unique_listeners` n'apprend donc pas à prédire le
#  succès : il repose un seuil sur la cible déguisée. C'est ce qui explique
#  l'AUC-ROC de 0,9921 de la variante V1, quand la variante V2 — audio seul —
#  plafonne à 0,5959, soit le niveau du hasard.
#
#  Ce job ne tranche pas à la place du modélisateur. Il fait deux choses :
#    · il MESURE la fuite (corrélations, § 6) au lieu de la supposer ;
#    · il RANGE chaque variable dans un groupe explicite, pour que le choix
#      des entrées en tâche 12 soit un acte conscient et documenté.
#
#  Les six groupes sont définis au § GROUPES ci-dessous.
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
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

CONFIG_AWS = Config(
    connect_timeout=5, read_timeout=30, retries={"max_attempts": 3, "mode": "standard"}
)

ANNEE_REFERENCE = 2026

# -----------------------------------------------------------------------------
#  GROUPES DE VARIABLES
#
#  A · AUDIO         Caractéristiques acoustiques Spotify. Mesurées sur le
#                    signal, totalement indépendantes de la popularité.
#                    Aucune fuite possible.
#
#  B · CONTEXTE      Métadonnées du titre. Ne contiennent aucune information
#                    sur la cible. Attention tout de même à `anciennete` :
#                    un titre ancien a eu plus de temps pour accumuler des
#                    écoutes — c'est un facteur de confusion, pas une fuite.
#
#  C · ARTISTE       Calculées sur les AUTRES titres du même artiste, la piste
#                    courante étant exclue du calcul. Disponibles avant toute
#                    écoute du nouveau titre, donc légitimes en production.
#                    MAIS elles dérivent des cibles d'autres lignes : la
#                    séparation train/test DOIT être groupée par artiste,
#                    sinon l'information du test fuit vers l'entraînement.
#
#  D · NOTORIÉTÉ     `nb_tags` compte les étiquettes Last.fm. Ce n'est pas
#                    dérivé de la cible, mais les étiquettes s'accumulent avec
#                    la popularité : c'est un indicateur indirect. À utiliser
#                    en le signalant.
#
#  E · ENGAGEMENT    Dérivées des mêmes lignes que la cible. FUITE DIRECTE.
#                    Conservées pour permettre l'étude d'ablation et pour
#                    chiffrer l'ampleur de la fuite.
#
#  F · CIBLE         `total_plays`, `log_total_plays`, `is_hit`.
#                    Jamais en entrée d'un modèle, sous aucun prétexte.
# -----------------------------------------------------------------------------
GROUPES = {
    "A_audio": [
        "danceability", "energy", "key", "loudness", "mode", "speechiness",
        "acousticness", "instrumentalness", "liveness", "valence", "tempo",
        "time_signature", "duration_ms",
    ],
    "B_contexte": ["anciennete", "duration_min", "artist_nb_titres_hors_piste"],
    "C_artiste": ["artist_plays_moyen_hors_piste", "artist_taux_hits_hors_piste"],
    "D_notoriete": ["nb_tags"],
    "E_engagement": [
        "unique_listeners", "avg_plays_par_auditeur", "ratio_engagement",
        "max_playcount",
    ],
    "F_cible": ["total_plays", "log_total_plays", "is_hit"],
}

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


def declarer_table_glue(glue, base, table, emplacement, schema, description, params=None):
    colonnes = [
        {"Name": c.name, "Type": TYPES_GLUE.get(str(c.dataType), "string")}
        for c in schema.fields
    ]
    definition = {
        "Name": table,
        "Description": description,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE",
                       "projet": "SoundLab", **(params or {})},
        "StorageDescriptor": {
            "Columns": colonnes, "Location": emplacement,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary":
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"}},
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
    p = argparse.ArgumentParser(description="Feature engineering SoundLab")
    p.add_argument("--music-info", required=True)
    p.add_argument("--listening-history", required=True)
    p.add_argument("--cible-engagement", required=True)
    p.add_argument("--cible-features", required=True)
    p.add_argument("--rapport", required=True)
    p.add_argument("--glue-db", required=True)
    p.add_argument("--percentile", type=float, default=0.75,
                   help="Seuil de définition du hit (0,75 = quartile supérieur)")
    p.add_argument("--region", default=os.environ.get("AWS_REGION", "eu-north-1"))
    args = p.parse_args()

    s3 = boto3.client("s3", region_name=args.region, config=CONFIG_AWS)
    glue = boto3.client("glue", region_name=args.region, config=CONFIG_AWS)

    spark = (
        SparkSession.builder
        .appName("soundlab-tache10-feature-engineering")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    journal(f"Spark {spark.version} démarré")

    rapport = {
        "tache": "10_feature_engineering",
        "horodatage_utc": datetime.now(timezone.utc).isoformat(),
        "percentile_cible": args.percentile,
    }

    music = spark.read.parquet(args.music_info)
    hist = spark.read.parquet(args.listening_history)

    # =========================================================================
    #  1. Agrégation de l'historique par piste
    #     Un seul parcours des 9,7 M de lignes, toutes les mesures en une
    #     expression agg() — même principe qu'en tâche 8.
    # =========================================================================
    journal("Agrégation de l'historique d'écoute par piste")

    engagement = (
        hist.groupBy("track_id").agg(
            F.sum("playcount").cast("long").alias("total_plays"),
            F.countDistinct("user_id_hash").alias("unique_listeners"),
            F.max("playcount").alias("max_playcount"),
            # Auditeurs ayant écouté plus de 5 fois : marqueur d'attachement
            # réel, par opposition à l'écoute unique de découverte.
            F.sum((F.col("playcount") > 5).cast("long")).alias("auditeurs_fideles"),
        )
        .withColumn("avg_plays_par_auditeur",
                    F.round(F.col("total_plays") / F.col("unique_listeners"), 4))
        .withColumn("ratio_engagement",
                    F.round(F.col("auditeurs_fideles") / F.col("unique_listeners"), 4))
        .withColumn("date_calcul", F.current_timestamp())
        .cache()
    )

    n_engagement = engagement.count()
    journal(f"{n_engagement:,} pistes agrégées")
    rapport["pistes_agregees"] = n_engagement

    # Écriture de l'agrégat — recommandation R3 de l'analyse Redshift.
    # Cette table de 30 000 lignes remplace, pour les requêtes métier, une
    # agrégation sur 9,7 millions de lignes.
    journal(f"Écriture de l'agrégat vers {args.cible_engagement}")
    engagement.coalesce(1).write.mode("overwrite").parquet(args.cible_engagement)
    declarer_table_glue(
        glue, args.glue_db, "track_engagement", args.cible_engagement.rstrip("/"),
        engagement.schema,
        "Agrégat d'engagement par piste — précalcul de la requête métier centrale",
        {"tache": "10"},
    )

    # =========================================================================
    #  2. Jointure avec les métadonnées
    #     Jointure interne : seules les pistes ayant au moins une écoute sont
    #     retenues. C'est le BIAIS DE SÉLECTION documenté en tâche 9 —
    #     60,1 % du catalogue. Le modèle ne répondra donc pas à « ce titre
    #     sera-t-il un succès » mais à « parmi les titres déjà écoutés au
    #     moins une fois, lequel atteindra le quartile supérieur ».
    #
    #     broadcast() sur l'agrégat : 30 000 lignes tiennent en mémoire de
    #     chaque exécuteur, ce qui évite un shuffle complet.
    # =========================================================================
    journal("Jointure métadonnées × engagement")

    base = music.join(F.broadcast(engagement), "track_id", "inner").cache()
    n_base = base.count()
    journal(f"{n_base:,} pistes retenues sur {music.count():,} du catalogue")
    rapport["pistes_retenues"] = n_base
    rapport["pistes_catalogue"] = music.count()
    rapport["couverture_pct"] = round(n_base / music.count() * 100, 2)

    # =========================================================================
    #  3. Construction de la cible
    #     approxQuantile avec une erreur relative nulle = calcul exact.
    #     Sur 30 000 lignes le coût est négligeable, et la reproductibilité
    #     du seuil compte davantage que la vitesse.
    # =========================================================================
    journal(f"Calcul du seuil au percentile {args.percentile}")

    seuil = base.approxQuantile("total_plays", [args.percentile], 0.0)[0]
    journal(f"Seuil retenu : {seuil:,.0f} écoutes cumulées")
    rapport["seuil_total_plays"] = float(seuil)

    base = (
        base
        .withColumn("is_hit", (F.col("total_plays") >= F.lit(seuil)).cast("int"))
        .withColumn("log_total_plays", F.round(F.log1p(F.col("total_plays")), 6))
        .withColumn("anciennete",
                    F.when(F.col("year").isNotNull(), F.lit(ANNEE_REFERENCE) - F.col("year")))
    )

    repartition = base.groupBy("is_hit").count().collect()
    dist = {int(r["is_hit"]): r["count"] for r in repartition}
    rapport["repartition_classes"] = {
        "hits": dist.get(1, 0),
        "niches": dist.get(0, 0),
        "taux_hits_pct": round(dist.get(1, 0) / n_base * 100, 2),
    }
    journal(f"{dist.get(1, 0):,} hits / {dist.get(0, 0):,} niches "
            f"({rapport['repartition_classes']['taux_hits_pct']} %)")

    # =========================================================================
    #  4. Variables d'artiste, calculées EN EXCLUANT la piste courante
    #
    #     Pour un titre donné, on veut savoir comment se comportent les AUTRES
    #     titres du même artiste. D'où la soustraction : on calcule la somme
    #     sur tout le groupe, puis on retire la contribution de la ligne.
    #
    #     Un artiste n'ayant qu'un seul titre reçoit NULL plutôt que 0 —
    #     mettre 0 inventerait un signal (« artiste sans succès ») là où
    #     l'information est simplement absente.
    #
    #     RAPPEL IMPORTANT pour la tâche 12 : ces variables dérivent des
    #     cibles d'autres lignes. La séparation train/test doit être GROUPÉE
    #     PAR ARTISTE, faute de quoi les étiquettes du jeu de test se
    #     retrouvent dans les variables d'entraînement.
    # =========================================================================
    journal("Variables d'artiste (calcul hors piste courante)")

    w_artiste = Window.partitionBy("artist")
    base = (
        base
        .withColumn("_artist_nb", F.count("*").over(w_artiste))
        .withColumn("_artist_plays", F.sum("total_plays").over(w_artiste))
        .withColumn("_artist_hits", F.sum("is_hit").over(w_artiste))
        .withColumn("artist_nb_titres_hors_piste", F.col("_artist_nb") - 1)
        .withColumn(
            "artist_plays_moyen_hors_piste",
            F.when(F.col("_artist_nb") > 1,
                   F.round((F.col("_artist_plays") - F.col("total_plays"))
                           / (F.col("_artist_nb") - 1), 2)))
        .withColumn(
            "artist_taux_hits_hors_piste",
            F.when(F.col("_artist_nb") > 1,
                   F.round((F.col("_artist_hits") - F.col("is_hit"))
                           / (F.col("_artist_nb") - 1), 4)))
        .drop("_artist_nb", "_artist_plays", "_artist_hits")
    )

    n_artiste_seul = base.filter(F.col("artist_plays_moyen_hors_piste").isNull()).count()
    rapport["pistes_artiste_unique"] = n_artiste_seul
    rapport["pistes_artiste_unique_pct"] = round(n_artiste_seul / n_base * 100, 2)
    journal(f"{n_artiste_seul:,} pistes dont l'artiste n'a qu'un titre "
            f"({rapport['pistes_artiste_unique_pct']} % — variables d'artiste à NULL)")

    n_artistes = base.select("artist").distinct().count()
    rapport["artistes_distincts"] = n_artistes
    journal(f"{n_artistes:,} artistes distincts — c'est la granularité de "
            f"regroupement à utiliser pour la séparation train/test")

    # =========================================================================
    #  5. Sélection et ordonnancement des colonnes
    # =========================================================================
    colonnes = (
        ["track_id", "name", "artist", "genre", "year"]
        + GROUPES["A_audio"]
        + GROUPES["B_contexte"]
        + GROUPES["C_artiste"]
        + GROUPES["D_notoriete"]
        + GROUPES["E_engagement"]
        + GROUPES["F_cible"]
        + ["date_calcul"]
    )
    features = base.select(*colonnes)

    # =========================================================================
    #  6. DIAGNOSTIC DE FUITE — le cœur de ce job
    #
    #     On corrèle chaque variable candidate avec la cible. Une corrélation
    #     proche de 1 avec `total_plays` signale une variable qui EST la cible
    #     sous un autre nom.
    # =========================================================================
    journal("Diagnostic de fuite — corrélations avec la cible")

    candidates = (GROUPES["A_audio"] + GROUPES["B_contexte"] + GROUPES["C_artiste"]
                  + GROUPES["D_notoriete"] + GROUPES["E_engagement"])

    correlations = {}
    for col in candidates:
        c_plays = features.stat.corr(col, "total_plays")
        c_hit = features.stat.corr(col, "is_hit")
        correlations[col] = {
            "corr_total_plays": round(c_plays, 4) if c_plays is not None else None,
            "corr_is_hit": round(c_hit, 4) if c_hit is not None else None,
            "groupe": next(g for g, cols in GROUPES.items() if col in cols),
        }
    rapport["correlations"] = correlations

    # Les trois corrélations qui démontrent la fuite
    c_ul = features.stat.corr("unique_listeners", "total_plays")
    c_log = features.stat.corr("unique_listeners", "log_total_plays")
    rapport["fuite_de_cible"] = {
        "corr_unique_listeners_total_plays": round(c_ul, 6),
        "corr_unique_listeners_log_total_plays": round(c_log, 6),
        "explication": (
            "total_plays = SUM(playcount) et unique_listeners = COUNT(*) sont "
            "calculés sur les mêmes lignes. Le playcount moyen valant 2,631, "
            f"le seuil de {seuil:,.0f} écoutes équivaut approximativement à un "
            f"seuil de {seuil / 2.631:,.0f} auditeurs."
        ),
    }
    journal(f"corr(unique_listeners, total_plays) = {c_ul:.6f}")
    journal(f"→ le seuil de {seuil:,.0f} écoutes équivaut à ~{seuil/2.631:,.0f} auditeurs")

    # Classement des variables sans fuite, par corrélation absolue avec la cible
    propres = {k: v for k, v in correlations.items()
               if v["groupe"] in ("A_audio", "B_contexte", "C_artiste", "D_notoriete")}
    top = sorted(propres.items(),
                 key=lambda kv: abs(kv[1]["corr_is_hit"] or 0), reverse=True)[:8]
    journal("Variables sans fuite les plus corrélées à is_hit :")
    for nom, v in top:
        journal(f"    {nom:38s} {v['corr_is_hit']:+.4f}  [{v['groupe']}]")

    # =========================================================================
    #  7. Statistiques descriptives — artefact pour la normalisation
    #
    #     Ces statistiques ne sont PAS appliquées aux données, pour deux
    #     raisons :
    #
    #     · Une forêt aléatoire est invariante à l'échelle. Un arbre découpe
    #       sur des seuils ; multiplier une colonne par mille ne change aucune
    #       décision. Normaliser avant un modèle à base d'arbres est un calcul
    #       sans effet.
    #
    #     · Les calibrer sur l'ensemble des données constituerait une seconde
    #       fuite, plus discrète : les moyennes et écarts-types du jeu de test
    #       se retrouveraient dans les paramètres appliqués à l'entraînement.
    #       Un scaler se calibre sur le train seul.
    #
    #     Elles sont donc persistées comme artefact documenté, utilisable par
    #     tout modèle sensible à l'échelle (régression logistique, réseau de
    #     neurones) à condition d'être recalculées sur le train.
    # =========================================================================
    journal("Statistiques descriptives (artefact de normalisation)")

    stats = {}
    for col in candidates:
        row = features.agg(
            F.avg(col).alias("moyenne"),
            F.stddev(col).alias("ecart_type"),
            F.min(col).alias("minimum"),
            F.max(col).alias("maximum"),
            F.expr(f"percentile_approx(`{col}`, 0.5)").alias("mediane"),
            F.avg(F.col(col).isNull().cast("double")).alias("taux_nuls"),
        ).first().asDict()
        stats[col] = {
            k: (round(float(v), 6) if v is not None else None) for k, v in row.items()
        }
    rapport["statistiques_descriptives"] = stats
    rapport["normalisation"] = {
        "appliquee": False,
        "justification": (
            "1) Une forêt aléatoire est invariante à toute transformation "
            "monotone des variables : normaliser n'aurait aucun effet. "
            "2) Calibrer un scaler sur l'ensemble des données ferait fuiter "
            "les statistiques du jeu de test vers l'entraînement. Les "
            "statistiques sont fournies pour un modèle sensible à l'échelle, "
            "à recalculer sur le seul jeu d'entraînement."
        ),
    }

    # =========================================================================
    #  8. Écriture
    # =========================================================================
    journal(f"Écriture de la table de features vers {args.cible_features}")
    features.coalesce(1).write.mode("overwrite").parquet(args.cible_features)

    declarer_table_glue(
        glue, args.glue_db, "songs_features_labeled",
        args.cible_features.rstrip("/"), features.schema,
        "Table d'entraînement — variables groupées par niveau de fuite (cf. rapport)",
        {"tache": "10", "avertissement_fuite": "groupe E_engagement = fuite de cible"},
    )

    rapport["groupes_variables"] = GROUPES
    rapport["colonnes_ecrites"] = len(colonnes)
    rapport["statut"] = "SUCCES"

    bucket, prefixe = decouper_s3(args.rapport)
    cle = f"{prefixe}/rapport_features_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.json"
    s3.put_object(
        Bucket=bucket, Key=cle,
        Body=json.dumps(rapport, indent=2, ensure_ascii=False, default=str).encode(),
        ContentType="application/json",
    )
    journal(f"Rapport : s3://{bucket}/{cle}")

    journal("Tâches 10-11 terminées")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
