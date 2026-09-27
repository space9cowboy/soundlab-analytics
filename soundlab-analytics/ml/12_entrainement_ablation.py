#!/usr/bin/env python3
# =============================================================================
#  SoundLab Analytics — Bloc 6 Big Data
#  Tâche 12 : entraînement du modèle et étude d'ablation
#
#  Entrée   s3://<curated>/songs_features_labeled/   30 459 pistes
#  Sorties  s3://<models>/ablation/                  modèles + rapport
#           mlruns/                                  suivi MLflow local
#
#  ---------------------------------------------------------------------------
#  POURQUOI CE JOB NE TOURNE PAS SUR EMR
#
#  30 459 lignes ne sont pas du Big Data. Provisionner des exécuteurs Spark et
#  subir deux minutes de démarrage à froid pour entraîner sur 30 000 lignes
#  coûterait plus que le calcul lui-même. Par ailleurs scikit-learn, MLflow et
#  SHAP ne figurent pas dans l'image EMR Serverless : les utiliser imposerait
#  d'empaqueter un environnement virtuel complet.
#
#  Le volume distribué (9,7 M de lignes) a été traité en Spark ; l'entraînement
#  se fait avec la pile ML standard. C'est un arbitrage, pas une facilité.
#
#  ---------------------------------------------------------------------------
#  DEUX PRÉCAUTIONS MÉTHODOLOGIQUES
#
#  1. SÉPARATION GROUPÉE PAR ARTISTE.
#     Les variables d'artiste sont calculées sur les AUTRES titres du même
#     artiste. Avec une séparation aléatoire, un titre du jeu de test
#     contribuerait aux variables d'un titre du jeu d'entraînement : les
#     étiquettes du test fuiteraient vers l'apprentissage. GroupShuffleSplit
#     sur `artist` garantit qu'un artiste est entièrement d'un côté ou de
#     l'autre.
#
#     Le job mesure d'ailleurs l'écart entre séparation groupée et séparation
#     aléatoire (§ 6). C'est une démonstration chiffrée de cette fuite.
#
#  2. VALEURS MANQUANTES TRAITÉES COMME UNE INFORMATION.
#     8,54 % des pistes ont des variables d'artiste nulles — leur artiste n'a
#     qu'un titre au catalogue. Imputer par la moyenne prétendrait que ces
#     artistes ont une performance moyenne, ce qui est faux : l'information
#     est absente, pas médiocre. On impute par une sentinelle hors domaine
#     accompagnée d'un indicateur explicite, laissant l'arbre décider si
#     l'absence elle-même porte du signal.
# =============================================================================

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
from botocore.config import Config
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, average_precision_score, confusion_matrix, f1_score,
    precision_score, recall_score, roc_auc_score, roc_curve,
)
from sklearn.model_selection import GroupKFold, GroupShuffleSplit, train_test_split

CONFIG_AWS = Config(
    connect_timeout=5, read_timeout=60, retries={"max_attempts": 3, "mode": "standard"}
)

GRAINE = 42
SENTINELLE = -1.0

# -----------------------------------------------------------------------------
#  Groupes de variables — repris de la tâche 10
# -----------------------------------------------------------------------------
AUDIO = [
    "danceability", "energy", "key", "loudness", "mode", "speechiness",
    "acousticness", "instrumentalness", "liveness", "valence", "tempo",
    "time_signature", "duration_ms",
]
CONTEXTE = ["anciennete", "duration_min", "artist_nb_titres_hors_piste"]
ARTISTE = ["artist_plays_moyen_hors_piste", "artist_taux_hits_hors_piste"]
NOTORIETE = ["nb_tags"]
ENGAGEMENT = ["unique_listeners", "avg_plays_par_auditeur", "ratio_engagement",
              "max_playcount"]

# -----------------------------------------------------------------------------
#  Les six variantes de l'ablation
#
#  Chacune répond à une question précise. Les deux dernières ne sont pas des
#  candidates : elles servent à établir l'ampleur de la fuite.
# -----------------------------------------------------------------------------
VARIANTES = [
    ("A_audio_seul", AUDIO,
     "Le signal acoustique suffit-il ? Reproduit la variante V2 du prototype."),
    ("B_audio_contexte", AUDIO + CONTEXTE,
     "Ancienneté, durée et taille du catalogue de l'artiste ajoutent-elles du signal ?"),
    ("C_audio_contexte_artiste", AUDIO + CONTEXTE + ARTISTE,
     "CANDIDAT RETENU — historique de l'artiste, hors piste courante. Aucune fuite."),
    ("D_avec_notoriete", AUDIO + CONTEXTE + ARTISTE + NOTORIETE,
     "nb_tags est un indicateur indirect de notoriété : apporte-t-il quelque chose ?"),
    ("E_unique_listeners_seul", AUDIO + ["unique_listeners"],
     "DIAGNOSTIC — reproduit la variante V1 du prototype. Teste si unique_listeners "
     "seule explique l'AUC de 0,9921."),
    ("F_engagement_complet", AUDIO + ENGAGEMENT,
     "DIAGNOSTIC — unique_listeners x avg_plays_par_auditeur = total_plays exactement. "
     "Teste la reconstruction algébrique de la cible."),
]


def journal(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def decouper_s3(uri: str):
    reste = uri.replace("s3://", "").rstrip("/")
    bucket, _, prefixe = reste.partition("/")
    return bucket, prefixe


def charger_depuis_s3(s3, uri: str) -> pd.DataFrame:
    """
    Télécharge les fichiers Parquet d'un préfixe S3 puis les lit avec pandas.

    On passe par un téléchargement explicite plutôt que par s3fs : une
    dépendance de moins, un comportement prévisible, et des messages d'erreur
    lisibles en cas de problème d'accès.
    """
    bucket, prefixe = decouper_s3(uri)
    tmp = Path(tempfile.mkdtemp(prefix="soundlab_"))
    n = 0
    paginateur = s3.get_paginator("list_objects_v2")
    for page in paginateur.paginate(Bucket=bucket, Prefix=prefixe + "/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                dest = tmp / Path(obj["Key"]).name
                s3.download_file(bucket, obj["Key"], str(dest))
                n += 1
    if n == 0:
        raise FileNotFoundError(f"Aucun fichier Parquet sous {uri}")
    journal(f"{n} fichier(s) Parquet téléchargé(s)")
    return pd.read_parquet(tmp)


def preparer(df: pd.DataFrame, colonnes: list):
    """
    Construit la matrice de variables.

    Les colonnes comportant des valeurs manquantes reçoivent un indicateur
    binaire dédié, puis sont remplies par une sentinelle hors domaine. L'arbre
    peut ainsi isoler les observations sans information au lieu de les
    confondre avec des observations moyennes.
    """
    X = df[colonnes].copy()
    ajoutees = []
    for col in colonnes:
        if X[col].isna().any():
            X[f"{col}_manquant"] = X[col].isna().astype(int)
            X[col] = X[col].fillna(SENTINELLE)
            ajoutees.append(f"{col}_manquant")
    return X, ajoutees


def evaluer(modele, X_test, y_test) -> dict:
    proba = modele.predict_proba(X_test)[:, 1]
    pred = (proba >= 0.5).astype(int)

    # Seuil atteignant l'objectif de rappel fixé par le cahier des charges
    fpr, tpr, seuils = roc_curve(y_test, proba)
    idx = np.searchsorted(tpr, 0.75, side="left")
    seuil_rappel_075 = float(seuils[min(idx, len(seuils) - 1)])
    pred_075 = (proba >= seuil_rappel_075).astype(int)

    mc = confusion_matrix(y_test, pred).tolist()
    return {
        "auc_roc": round(float(roc_auc_score(y_test, proba)), 4),
        "auc_pr": round(float(average_precision_score(y_test, proba)), 4),
        "accuracy": round(float(accuracy_score(y_test, pred)), 4),
        "precision": round(float(precision_score(y_test, pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_test, pred, zero_division=0)), 4),
        "f1": round(float(f1_score(y_test, pred, zero_division=0)), 4),
        "matrice_confusion": mc,
        "seuil_pour_rappel_075": round(seuil_rappel_075, 4),
        "precision_a_rappel_075": round(
            float(precision_score(y_test, pred_075, zero_division=0)), 4),
        "recall_a_rappel_075": round(
            float(recall_score(y_test, pred_075, zero_division=0)), 4),
    }


def entrainer(X, y, groupes, test_size=0.2, graine=GRAINE, groupe=True):
    """Sépare, entraîne, évalue. `groupe=False` produit la séparation naïve."""
    if groupe:
        gss = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=graine)
        i_train, i_test = next(gss.split(X, y, groups=groupes))
    else:
        i_train, i_test = train_test_split(
            np.arange(len(X)), test_size=test_size, random_state=graine, stratify=y)

    X_tr, X_te = X.iloc[i_train], X.iloc[i_test]
    y_tr, y_te = y.iloc[i_train], y.iloc[i_test]

    modele = RandomForestClassifier(
        n_estimators=300,
        max_depth=None,
        min_samples_leaf=5,          # limite le surapprentissage sur 30 000 lignes
        class_weight="balanced_subsample",   # 25 % de hits : déséquilibre modéré
        n_jobs=-1,
        random_state=graine,
    )
    modele.fit(X_tr, y_tr)
    return modele, X_te, y_te, len(i_train), len(i_test)


def main() -> int:
    p = argparse.ArgumentParser(description="Entraînement et ablation SoundLab")
    p.add_argument("--features", required=True, help="s3://<curated>/songs_features_labeled/")
    p.add_argument("--sortie", required=True, help="s3://<models>/ablation/")
    p.add_argument("--rapport", required=True, help="s3://<logs>/ml/tache12/")
    p.add_argument("--mlruns", default="./mlruns")
    p.add_argument("--region", default=os.environ.get("AWS_REGION", "eu-north-1"))
    args = p.parse_args()

    s3 = boto3.client("s3", region_name=args.region, config=CONFIG_AWS)

    journal("Chargement de la table de features")
    df = charger_depuis_s3(s3, args.features)
    journal(f"{len(df):,} pistes, {df.shape[1]} colonnes")

    y = df["is_hit"]
    groupes = df["artist"]
    journal(f"{groupes.nunique():,} artistes distincts — granularité de regroupement")
    journal(f"Répartition : {int(y.sum()):,} hits / {int((1 - y).sum()):,} niches "
            f"({y.mean() * 100:.2f} %)")

    # -------------------------------------------------------------------------
    #  Suivi MLflow
    #
    #  Le stockage sur fichiers ("file://...") est passé en mode maintenance
    #  dans les versions récentes de MLflow et lève une exception. On utilise
    #  donc une base SQLite — qui reste un fichier unique, versionnable et sans
    #  serveur, mais reçoit les évolutions du produit.
    #
    #  MLflow reste facultatif : son absence ne doit pas empêcher l'ablation.
    # -------------------------------------------------------------------------
    suivi = False
    try:
        import mlflow
        rep = Path(args.mlruns).resolve()
        rep.mkdir(parents=True, exist_ok=True)
        mlflow.set_tracking_uri(f"sqlite:///{rep / 'mlflow.db'}")
        mlflow.set_experiment("soundlab-ablation")
        suivi = True
        journal(f"Suivi MLflow actif — sqlite:///{rep / 'mlflow.db'}")
    except Exception as err:
        journal(f"MLflow indisponible ({err}) — poursuite sans suivi")

    rapport = {
        "tache": "12_entrainement_ablation",
        "horodatage_utc": datetime.now(timezone.utc).isoformat(),
        "pistes": len(df),
        "artistes": int(groupes.nunique()),
        "taux_hits_pct": round(float(y.mean()) * 100, 2),
        "separation": "GroupShuffleSplit par artiste, 20 % de test",
        "modele": {
            "type": "RandomForestClassifier",
            "n_estimators": 300,
            "min_samples_leaf": 5,
            "class_weight": "balanced_subsample",
            "random_state": GRAINE,
        },
        "variantes": {},
    }

    # =========================================================================
    #  Ablation
    # =========================================================================
    resultats = []
    for nom, colonnes, question in VARIANTES:
        journal(f"── {nom} — {len(colonnes)} variables")
        X, ajoutees = preparer(df, colonnes)
        modele, X_te, y_te, n_tr, n_te = entrainer(X, y, groupes)
        m = evaluer(modele, X_te, y_te)

        importances = sorted(
            zip(X.columns, modele.feature_importances_),
            key=lambda kv: kv[1], reverse=True)[:8]

        rapport["variantes"][nom] = {
            "question": question,
            "variables": colonnes,
            "indicateurs_manquants": ajoutees,
            "n_entrainement": n_tr,
            "n_test": n_te,
            "metriques": m,
            "importances": [{"variable": v, "poids": round(float(w), 4)}
                            for v, w in importances],
        }
        resultats.append((nom, m["auc_roc"], m["recall"], m["precision"]))
        journal(f"   AUC-ROC {m['auc_roc']:.4f} · rappel {m['recall']:.4f} "
                f"· précision {m['precision']:.4f}")
        journal(f"   première variable : {importances[0][0]} ({importances[0][1]:.3f})")

        if suivi:
            import mlflow
            with mlflow.start_run(run_name=nom):
                mlflow.log_param("variables", ",".join(colonnes))
                mlflow.log_param("nb_variables", len(X.columns))
                mlflow.log_param("separation", "GroupShuffleSplit(artist)")
                for k, v in m.items():
                    if isinstance(v, (int, float)):
                        mlflow.log_metric(k, v)
                mlflow.sklearn.log_model(modele, "modele")

    # =========================================================================
    #  Démonstration de la fuite liée à la séparation
    #
    #  Même variante, même modèle, même graine — seule la méthode de séparation
    #  change. L'écart mesure exactement ce que les variables d'artiste font
    #  fuiter quand les titres d'un même artiste se répartissent des deux côtés.
    # =========================================================================
    journal("── Diagnostic : séparation groupée contre séparation aléatoire")
    X, _ = preparer(df, AUDIO + CONTEXTE + ARTISTE)

    m_g, Xg, yg, _, _ = entrainer(X, y, groupes, groupe=True)
    auc_groupe = evaluer(m_g, Xg, yg)["auc_roc"]

    m_a, Xa, ya, _, _ = entrainer(X, y, groupes, groupe=False)
    auc_aleatoire = evaluer(m_a, Xa, ya)["auc_roc"]

    rapport["diagnostic_separation"] = {
        "variante": "C_audio_contexte_artiste",
        "auc_separation_groupee_par_artiste": auc_groupe,
        "auc_separation_aleatoire": auc_aleatoire,
        "ecart": round(auc_aleatoire - auc_groupe, 4),
        "interpretation": (
            "L'écart mesure la fuite introduite par une séparation aléatoire : "
            "les variables d'artiste étant calculées sur les autres titres du "
            "même artiste, une répartition non groupée fait passer les "
            "étiquettes du jeu de test dans les variables d'entraînement. "
            "Seule la valeur groupée est publiable."
        ),
    }
    journal(f"   groupée   AUC {auc_groupe:.4f}")
    journal(f"   aléatoire AUC {auc_aleatoire:.4f}  (écart {auc_aleatoire - auc_groupe:+.4f})")

    # =========================================================================
    #  Validation croisée sur les variantes candidates
    #
    #  Une séparation unique ne dit rien de la stabilité du résultat : un
    #  AUC de 0,7953 face à un objectif de 0,80 n'est interprétable qu'assorti
    #  de sa dispersion. Cinq plis groupés par artiste donnent une moyenne et
    #  un écart-type, donc une réponse honnête à « l'objectif est-il atteint ».
    #
    #  GroupKFold garantit qu'aucun artiste n'apparaît dans deux plis.
    # =========================================================================
    journal("── Validation croisée, 5 plis groupés par artiste")

    modele_params = dict(
        n_estimators=300, max_depth=None, min_samples_leaf=5,
        class_weight="balanced_subsample", n_jobs=-1, random_state=GRAINE,
    )
    rapport["validation_croisee"] = {}

    for nom, colonnes, _ in VARIANTES:
        if not (nom.startswith("C_") or nom.startswith("D_")):
            continue
        Xc, _ = preparer(df, colonnes)
        gkf = GroupKFold(n_splits=5)
        aucs, rappels, precisions = [], [], []
        for i_tr, i_te in gkf.split(Xc, y, groups=groupes):
            mod = RandomForestClassifier(**modele_params)
            mod.fit(Xc.iloc[i_tr], y.iloc[i_tr])
            met = evaluer(mod, Xc.iloc[i_te], y.iloc[i_te])
            aucs.append(met["auc_roc"])
            rappels.append(met["recall"])
            precisions.append(met["precision"])

        moy, ect = float(np.mean(aucs)), float(np.std(aucs))
        rapport["validation_croisee"][nom] = {
            "auc_par_pli": [round(a, 4) for a in aucs],
            "auc_moyen": round(moy, 4),
            "auc_ecart_type": round(ect, 4),
            "auc_intervalle": [round(moy - 2 * ect, 4), round(moy + 2 * ect, 4)],
            "rappel_moyen": round(float(np.mean(rappels)), 4),
            "precision_moyenne": round(float(np.mean(precisions)), 4),
            "objectif_080_atteint": bool(moy >= 0.80),
            "objectif_080_dans_intervalle": bool(moy + 2 * ect >= 0.80),
        }
        journal(f"   {nom:<28} AUC {moy:.4f} ± {ect:.4f} "
                f"(plis : {', '.join(f'{a:.3f}' for a in aucs)})")

    # =========================================================================
    #  Synthèse
    # =========================================================================
    print("", flush=True)
    journal("SYNTHÈSE DE L'ABLATION")
    print(f"      {'Variante':<28} {'AUC-ROC':>9} {'Rappel':>9} {'Précision':>10} "
          f"{'Préc.@rappel .75':>17}", flush=True)
    print("      " + "-" * 78, flush=True)
    for nom, auc, rec, prec in resultats:
        marque = " ←" if nom.startswith("C_") else ""
        p75 = rapport["variantes"][nom]["metriques"]["precision_a_rappel_075"]
        print(f"      {nom:<28} {auc:>9.4f} {rec:>9.4f} {prec:>10.4f} "
              f"{p75:>17.4f}{marque}", flush=True)

    if rapport["validation_croisee"]:
        print("", flush=True)
        print(f"      {'Validation croisée (5 plis)':<28} {'AUC moyen':>12} {'± écart-type':>14}",
              flush=True)
        print("      " + "-" * 56, flush=True)
        for nom, v in rapport["validation_croisee"].items():
            print(f"      {nom:<28} {v['auc_moyen']:>12.4f} {v['auc_ecart_type']:>14.4f}",
                  flush=True)

    cand = "C_audio_contexte_artiste"
    cv = rapport["validation_croisee"].get(cand, {})
    rapport["objectifs"] = {
        "auc_roc_cible": 0.80,
        "recall_cible": 0.75,
        "variante_candidate": cand,
        "auc_separation_unique": rapport["variantes"][cand]["metriques"]["auc_roc"],
        "auc_validation_croisee": cv.get("auc_moyen"),
        "auc_ecart_type": cv.get("auc_ecart_type"),
        "objectif_auc_atteint": cv.get("objectif_080_atteint"),
        "note_rappel": (
            "Le rappel n'est pas une propriété du modèle mais du seuil de "
            "décision. L'objectif de 0,75 est atteignable en abaissant le "
            "seuil ; la grandeur à arbitrer est la précision conservée à ce "
            "niveau de rappel (champ precision_a_rappel_075)."
        ),
    }
    rapport["statut"] = "SUCCES"

    bucket, prefixe = decouper_s3(args.rapport)
    cle = f"{prefixe}/rapport_ablation_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.json"
    s3.put_object(
        Bucket=bucket, Key=cle,
        Body=json.dumps(rapport, indent=2, ensure_ascii=False, default=str).encode(),
        ContentType="application/json",
    )
    journal(f"Rapport : s3://{bucket}/{cle}")

    journal("Tâche 12 terminée")
    return 0


if __name__ == "__main__":
    sys.exit(main())
