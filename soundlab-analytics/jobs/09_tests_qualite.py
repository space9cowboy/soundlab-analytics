#!/usr/bin/env python3
# =============================================================================
#  SoundLab Analytics — Bloc 6 Big Data
#  Tâche 9 : suite de tests de qualité des données
#
#  Lit les deux tables curated et exécute une batterie de contrôles déclarés,
#  chacun avec un seuil justifié et un niveau de sévérité.
#
#  ---------------------------------------------------------------------------
#  POURQUOI CE JOB EXISTE SÉPARÉMENT DES PIPELINES D'INGESTION
#
#  Les tâches 7 et 8 mesurent la qualité de ce qu'elles produisent — c'est
#  utile, mais insuffisant : un pipeline qui s'auto-évalue ne peut pas
#  détecter une dérive entre deux tables, ni une régression introduite par
#  une exécution ultérieure. Ce job lit l'état RÉEL de la couche curated,
#  quel que soit ce qui l'a produite et quand.
#
#  Il se termine avec un code de sortie non nul si un contrôle BLOQUANT
#  échoue. C'est ce qui en fait une *porte de qualité* : branché dans Airflow
#  en tâche 14, il empêchera le feature engineering de tourner sur des
#  données invalides plutôt que de propager le problème jusqu'au modèle.
#
#  Chaque seuil est une décision documentée, pas une valeur ronde choisie au
#  hasard : un contrôle dont personne ne peut justifier le seuil finit par
#  être désactivé au premier faux positif.
#  ---------------------------------------------------------------------------
# =============================================================================

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CONFIG_AWS = Config(
    connect_timeout=5, read_timeout=30, retries={"max_attempts": 3, "mode": "standard"}
)

BLOQUANT = "BLOQUANT"
AVERTISSEMENT = "AVERTISSEMENT"

# -----------------------------------------------------------------------------
# Seuils — chacun avec sa justification
# -----------------------------------------------------------------------------
SEUILS = {
    # Le dataset source annonce 50 683 pistes. Une perte de plus de 1 %
    # signalerait un filtre trop agressif dans l'ingestion.
    "lignes_min_music_info": 50_000,
    # 9,7 M triplets annoncés ; même raisonnement.
    "lignes_min_historique": 9_600_000,
    # `genre` est nul à 55,9 % dans la source. Le seuil est fixé juste
    # au-dessus : il n'alerte pas sur l'état connu, mais signale une
    # dégradation. Simple avertissement — le modèle retenu n'utilise pas
    # cette colonne.
    "taux_nuls_genre_max": 60.0,
    # Les 13 caractéristiques audio sont complètes à 100 %. La moindre
    # valeur nulle casserait le modèle : tolérance zéro.
    "taux_nuls_audio_max": 0.0,
    # Longueur du jeton pseudonyme après troncature à 128 bits.
    "longueur_jeton": 32,
    # Couverture des pistes par l'historique d'écoute : 60,1 % mesuré.
    # En dessous de 55 %, l'échantillon d'entraînement deviendrait trop
    # étroit pour rester représentatif du catalogue.
    "couverture_min_pct": 55.0,
}

# Bornes documentées de l'API Spotify
BORNES = {
    "danceability": (0.0, 1.0), "energy": (0.0, 1.0), "speechiness": (0.0, 1.0),
    "acousticness": (0.0, 1.0), "instrumentalness": (0.0, 1.0),
    "liveness": (0.0, 1.0), "valence": (0.0, 1.0),
    "loudness": (-60.0, 5.0), "tempo": (0.0, 250.0),
    "key": (-1, 11), "mode": (0, 1), "time_signature": (0, 7),
}

AUDIO = [
    "danceability", "energy", "key", "loudness", "mode", "speechiness",
    "acousticness", "instrumentalness", "liveness", "valence", "tempo",
    "time_signature", "duration_ms",
]


def journal(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def decouper_s3(uri: str):
    reste = uri.replace("s3://", "").rstrip("/")
    bucket, _, prefixe = reste.partition("/")
    return bucket, prefixe


class Suite:
    """Registre des contrôles et de leurs résultats."""

    def __init__(self):
        self.resultats = []

    def verifier(self, nom, categorie, severite, attendu, obtenu, ok, note=""):
        self.resultats.append({
            "controle": nom,
            "categorie": categorie,
            "severite": severite,
            "attendu": attendu,
            "obtenu": obtenu,
            "statut": "REUSSI" if ok else "ECHOUE",
            "note": note,
        })
        marque = "✓" if ok else ("✗" if severite == BLOQUANT else "!")
        print(f"  {marque} [{categorie}] {nom}", flush=True)
        print(f"      attendu : {attendu}", flush=True)
        print(f"      obtenu  : {obtenu}", flush=True)
        return ok

    @property
    def echecs_bloquants(self):
        return [r for r in self.resultats
                if r["statut"] == "ECHOUE" and r["severite"] == BLOQUANT]

    @property
    def avertissements(self):
        return [r for r in self.resultats
                if r["statut"] == "ECHOUE" and r["severite"] == AVERTISSEMENT]


def main() -> int:
    p = argparse.ArgumentParser(description="Tests de qualité — couche curated")
    p.add_argument("--music-info",       required=True)
    p.add_argument("--listening-history", required=True)
    p.add_argument("--rapport",          required=True)
    p.add_argument("--region", default=os.environ.get("AWS_REGION", "eu-north-1"))
    args = p.parse_args()

    s3 = boto3.client("s3", region_name=args.region, config=CONFIG_AWS)

    spark = (
        SparkSession.builder
        .appName("soundlab-tache09-tests-qualite")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    journal(f"Spark {spark.version} démarré")

    suite = Suite()

    music = spark.read.parquet(args.music_info).cache()
    hist  = spark.read.parquet(args.listening_history).cache()

    # =========================================================================
    #  A. Structure et volumétrie — music_info
    #     Un seul agg() : toute la campagne en un parcours.
    # =========================================================================
    journal("A. Contrôles sur music_info")

    exprs = [
        F.count("*").alias("lignes"),
        F.countDistinct("track_id").alias("track_id_distincts"),
        F.sum(F.col("track_id").isNull().cast("long")).alias("track_id_nuls"),
        F.round(F.avg(F.col("genre").isNull().cast("double")) * 100, 3).alias("nuls_genre_pct"),
        F.round(F.avg(F.col("year").isNull().cast("double")) * 100, 3).alias("nuls_year_pct"),
    ]
    # Nullité des caractéristiques audio, colonne par colonne
    exprs += [
        F.sum(F.col(c).isNull().cast("long")).alias(f"nuls_{c}") for c in AUDIO
    ]
    # Valeurs hors bornes Spotify
    exprs += [
        F.sum(((F.col(c) < mini) | (F.col(c) > maxi)).cast("long")).alias(f"hb_{c}")
        for c, (mini, maxi) in BORNES.items()
    ]
    m = music.agg(*exprs).first().asDict()

    suite.verifier(
        "Volumétrie de music_info", "volumétrie", BLOQUANT,
        f"≥ {SEUILS['lignes_min_music_info']:,} lignes",
        f"{m['lignes']:,} lignes",
        m["lignes"] >= SEUILS["lignes_min_music_info"],
    )

    suite.verifier(
        "Unicité de la clé métier track_id", "intégrité", BLOQUANT,
        "0 doublon, 0 valeur nulle",
        f"{m['lignes'] - m['track_id_distincts']} doublon(s), {m['track_id_nuls']} nul(s)",
        m["lignes"] == m["track_id_distincts"] and m["track_id_nuls"] == 0,
        "Une clé dupliquée fausserait la jointure de la tâche 10 en multipliant des lignes.",
    )

    nuls_audio = {c: m[f"nuls_{c}"] for c in AUDIO if m[f"nuls_{c}"] > 0}
    suite.verifier(
        "Complétude des 13 caractéristiques audio", "complétude", BLOQUANT,
        f"taux de nuls ≤ {SEUILS['taux_nuls_audio_max']} %",
        "aucune valeur nulle" if not nuls_audio else str(nuls_audio),
        not nuls_audio,
        "Ces colonnes sont les variables explicatives du modèle retenu (V1).",
    )

    hors_bornes = {c: m[f"hb_{c}"] for c in BORNES if m[f"hb_{c}"] > 0}
    suite.verifier(
        "Respect des bornes de l'API Spotify", "vraisemblance", BLOQUANT,
        "aucune valeur hors bornes documentées",
        "conforme" if not hors_bornes else str(hors_bornes),
        not hors_bornes,
        "Bornes issues de la documentation Spotify Audio Features.",
    )

    suite.verifier(
        "Taux de valeurs nulles sur genre", "complétude", AVERTISSEMENT,
        f"≤ {SEUILS['taux_nuls_genre_max']} %",
        f"{m['nuls_genre_pct']} %",
        m["nuls_genre_pct"] <= SEUILS["taux_nuls_genre_max"],
        "Colonne non utilisée par le modèle V1 : son incomplétude explique "
        "l'échec de la variante V3 (AUC-ROC 0,6579).",
    )

    suite.verifier(
        "Taux de valeurs nulles sur year", "complétude", AVERTISSEMENT,
        "documenté, sans seuil bloquant",
        f"{m['nuls_year_pct']} %",
        True,
        "Les années à 0 dans le MSD ont été converties en NULL à l'ingestion.",
    )

    # =========================================================================
    #  B. Historique d'écoute et conformité RGPD
    # =========================================================================
    journal("B. Contrôles sur listening_history")

    colonnes_hist = set(hist.columns)

    # -- Contrôle de conformité : aucun identifiant en clair ne doit subsister
    interdits = {"user_id", "userid", "user", "email", "ip"} & colonnes_hist
    suite.verifier(
        "Absence d'identifiant direct dans la couche curated", "RGPD", BLOQUANT,
        "aucune colonne d'identification directe",
        "conforme" if not interdits else f"colonnes interdites : {sorted(interdits)}",
        not interdits,
        "Minimisation (art. 5.1.c RGPD) : l'identifiant en clair ne doit exister "
        "nulle part en aval de l'ingestion.",
    )

    h = hist.agg(
        F.count("*").alias("lignes"),
        F.countDistinct("user_id_hash").alias("jetons"),
        F.countDistinct("track_id").alias("pistes"),
        F.min("playcount").alias("playcount_min"),
        F.max("playcount").alias("playcount_max"),
        F.sum((F.col("playcount") <= 0).cast("long")).alias("playcount_non_positif"),
        F.sum(F.col("user_id_hash").isNull().cast("long")).alias("jetons_nuls"),
        F.min(F.length("user_id_hash")).alias("jeton_len_min"),
        F.max(F.length("user_id_hash")).alias("jeton_len_max"),
        F.sum((~F.col("user_id_hash").rlike("^[0-9a-f]+$")).cast("long")).alias("jetons_non_hex"),
    ).first().asDict()

    suite.verifier(
        "Volumétrie de listening_history", "volumétrie", BLOQUANT,
        f"≥ {SEUILS['lignes_min_historique']:,} lignes",
        f"{h['lignes']:,} lignes",
        h["lignes"] >= SEUILS["lignes_min_historique"],
    )

    suite.verifier(
        "Format du jeton pseudonyme", "RGPD", BLOQUANT,
        f"{SEUILS['longueur_jeton']} caractères hexadécimaux, aucun nul",
        f"longueurs {h['jeton_len_min']}–{h['jeton_len_max']}, "
        f"{h['jetons_non_hex']} non hexadécimal(aux), {h['jetons_nuls']} nul(s)",
        (h["jeton_len_min"] == h["jeton_len_max"] == SEUILS["longueur_jeton"]
         and h["jetons_non_hex"] == 0 and h["jetons_nuls"] == 0),
        "Une longueur hétérogène signalerait un défaut de la fonction de hachage.",
    )

    suite.verifier(
        "Positivité de playcount", "vraisemblance", BLOQUANT,
        "toutes les valeurs ≥ 1",
        f"min {h['playcount_min']}, max {h['playcount_max']}, "
        f"{h['playcount_non_positif']} valeur(s) ≤ 0",
        h["playcount_non_positif"] == 0,
        "Un compte d'écoutes nul ou négatif n'a pas de sens métier.",
    )

    # -- Doublons sur la clé composite : un seul shuffle
    journal("  calcul des doublons (user_id_hash, track_id)")
    doublons = (
        hist.groupBy("user_id_hash", "track_id").count()
            .where(F.col("count") > 1).count()
    )
    suite.verifier(
        "Unicité de la clé composite (user_id_hash, track_id)", "intégrité", BLOQUANT,
        "0 doublon",
        f"{doublons} doublon(s)",
        doublons == 0,
        "Un doublon gonflerait artificiellement total_plays pour la piste concernée.",
    )

    # =========================================================================
    #  C. Intégrité référentielle entre les deux tables
    #     C'est le seul contrôle qu'aucun des deux pipelines ne pouvait faire :
    #     il porte sur la relation, pas sur une table.
    # =========================================================================
    journal("C. Intégrité référentielle")

    pistes_hist = hist.select("track_id").distinct()
    pistes_music = music.select("track_id")

    orphelins = pistes_hist.join(pistes_music, "track_id", "left_anti").count()
    suite.verifier(
        "Intégrité référentielle listening_history → music_info", "intégrité", BLOQUANT,
        "0 track_id orphelin",
        f"{orphelins} orphelin(s) sur {h['pistes']:,} pistes écoutées",
        orphelins == 0,
        "Un orphelin serait perdu silencieusement lors de la jointure de la tâche 10.",
    )

    couverture = round(h["pistes"] / m["lignes"] * 100, 2)
    suite.verifier(
        "Couverture du catalogue par l'historique d'écoute", "représentativité", AVERTISSEMENT,
        f"≥ {SEUILS['couverture_min_pct']} % du catalogue",
        f"{couverture} % ({h['pistes']:,} sur {m['lignes']:,} pistes)",
        couverture >= SEUILS["couverture_min_pct"],
        "BIAIS DE SÉLECTION : le modèle ne sera entraîné que sur les pistes ayant "
        "reçu au moins une écoute. Il ne répond donc pas à « cette chanson sera-t-elle "
        "un succès » mais à « parmi les chansons déjà écoutées, laquelle atteindra le "
        "quartile supérieur ». À énoncer explicitement dans le rapport.",
    )

    # =========================================================================
    #  D. Rapport
    # =========================================================================
    total = len(suite.resultats)
    reussis = sum(1 for r in suite.resultats if r["statut"] == "REUSSI")

    rapport = {
        "tache": "09_tests_qualite",
        "horodatage_utc": datetime.now(timezone.utc).isoformat(),
        "tables_controlees": {
            "music_info": args.music_info,
            "listening_history": args.listening_history,
        },
        "seuils": SEUILS,
        "synthese": {
            "controles": total,
            "reussis": reussis,
            "echecs_bloquants": len(suite.echecs_bloquants),
            "avertissements": len(suite.avertissements),
        },
        "resultats": suite.resultats,
        "statistiques": {
            "music_info_lignes": m["lignes"],
            "music_info_nuls_genre_pct": m["nuls_genre_pct"],
            "historique_lignes": h["lignes"],
            "historique_jetons_distincts": h["jetons"],
            "historique_pistes_distinctes": h["pistes"],
            "couverture_catalogue_pct": couverture,
            "pistes_sans_historique": m["lignes"] - h["pistes"],
        },
    }
    rapport["statut"] = "ECHEC" if suite.echecs_bloquants else "SUCCES"

    bucket, prefixe = decouper_s3(args.rapport)
    cle = f"{prefixe}/rapport_tests_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.json"
    s3.put_object(
        Bucket=bucket, Key=cle,
        Body=json.dumps(rapport, indent=2, ensure_ascii=False, default=str).encode(),
        ContentType="application/json",
    )

    print("", flush=True)
    journal(f"SYNTHÈSE — {reussis}/{total} contrôles réussis, "
            f"{len(suite.echecs_bloquants)} échec(s) bloquant(s), "
            f"{len(suite.avertissements)} avertissement(s)")
    journal(f"Rapport : s3://{bucket}/{cle}")

    for r in suite.avertissements:
        journal(f"AVERTISSEMENT — {r['controle']} : {r['obtenu']}")
    for r in suite.echecs_bloquants:
        journal(f"ÉCHEC BLOQUANT — {r['controle']} : {r['obtenu']}")

    spark.stop()

    # Code de sortie non nul : c'est ce qui fait de ce job une porte de qualité
    # exploitable par un orchestrateur.
    return 1 if suite.echecs_bloquants else 0


if __name__ == "__main__":
    sys.exit(main())
