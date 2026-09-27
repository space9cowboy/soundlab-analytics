#!/usr/bin/env python3
# =============================================================================
#  SoundLab Analytics — Bloc 6 Big Data
#  Tâche 13 : évaluation et interprétabilité du modèle retenu
#
#  Entrée   s3://<curated>/songs_features_labeled/
#  Sorties  s3://<models>/evaluation/   graphiques PNG
#           s3://<logs>/ml/tache13/     rapport JSON
#           ./rapports/                 copies locales des graphiques
#
#  ---------------------------------------------------------------------------
#  POURQUOI RÉENTRAÎNER PLUTÔT QUE CHARGER LE MODÈLE FINAL
#
#  Le modèle publié en tâche 12b a été réentraîné sur l'INTÉGRALITÉ des
#  données — c'est ce qu'on veut en production, mais cela interdit toute
#  évaluation : il n'existe plus d'observation qu'il n'ait pas vue.
#
#  Ce job reproduit donc la séparation de l'ablation (GroupShuffleSplit,
#  graine 42, 20 % de test, regroupement par artiste), entraîne sur le train
#  et évalue sur le test. Les courbes et les valeurs SHAP portent ainsi
#  exclusivement sur des observations jamais vues.
#
#  ---------------------------------------------------------------------------
#  CE QUE SHAP APPORTE QUE LES IMPORTANCES N'APPORTENT PAS
#
#  `feature_importances_` d'une forêt mesure la réduction moyenne d'impureté :
#  une grandeur globale, positive par construction, et biaisée en faveur des
#  variables à forte cardinalité. Elle dit qu'une variable compte, jamais
#  comment ni dans quel sens.
#
#  SHAP attribue à chaque PRÉDICTION la contribution de chaque variable, avec
#  un signe. On peut donc répondre à « une valeur élevée de cette variable
#  pousse-t-elle vers le hit ou vers la niche », et voir si l'effet est
#  monotone ou s'inverse selon la plage — ce qu'aucune importance globale ne
#  permet.
#
#  Le job compare explicitement les deux classements (§ 5) : leurs désaccords
#  sont eux-mêmes une information.
# =============================================================================

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import matplotlib
matplotlib.use("Agg")          # aucun affichage : on écrit des fichiers
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from botocore.config import Config
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    ConfusionMatrixDisplay, average_precision_score, confusion_matrix,
    precision_recall_curve, precision_score, recall_score, roc_auc_score, roc_curve,
)
from sklearn.model_selection import GroupShuffleSplit

CONFIG_AWS = Config(
    connect_timeout=5, read_timeout=60, retries={"max_attempts": 3, "mode": "standard"}
)

GRAINE = 42
SENTINELLE = -1.0
RAPPEL_CIBLE = 0.75
N_ECHANTILLON_SHAP = 3000      # voir § 4 pour la justification

AUDIO = [
    "danceability", "energy", "key", "loudness", "mode", "speechiness",
    "acousticness", "instrumentalness", "liveness", "valence", "tempo",
    "time_signature", "duration_ms",
]
CONTEXTE = ["anciennete", "duration_min", "artist_nb_titres_hors_piste"]
ARTISTE = ["artist_plays_moyen_hors_piste", "artist_taux_hits_hors_piste"]
VARIABLES = AUDIO + CONTEXTE + ARTISTE

ETIQUETTES = {
    "artist_taux_hits_hors_piste": "Taux de hits des autres titres de l'artiste",
    "artist_plays_moyen_hors_piste": "Écoutes moyennes des autres titres",
    "artist_nb_titres_hors_piste": "Nombre d'autres titres au catalogue",
    "anciennete": "Ancienneté du titre (années)",
    "duration_min": "Durée (minutes)",
    "duration_ms": "Durée (ms)",
    "danceability": "Dansabilité",
    "energy": "Énergie",
    "loudness": "Volume sonore",
    "speechiness": "Présence de paroles parlées",
    "acousticness": "Caractère acoustique",
    "instrumentalness": "Caractère instrumental",
    "liveness": "Captation live",
    "valence": "Valence (positivité)",
    "tempo": "Tempo",
    "key": "Tonalité",
    "mode": "Mode (majeur/mineur)",
    "time_signature": "Signature rythmique",
}


def journal(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def decouper_s3(uri: str):
    reste = uri.replace("s3://", "").rstrip("/")
    bucket, _, prefixe = reste.partition("/")
    return bucket, prefixe


def charger_depuis_s3(s3, uri: str) -> pd.DataFrame:
    bucket, prefixe = decouper_s3(uri)
    tmp = Path(tempfile.mkdtemp(prefix="soundlab_"))
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket,
                                                             Prefix=prefixe + "/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                s3.download_file(bucket, obj["Key"], str(tmp / Path(obj["Key"]).name))
                n += 1
    if n == 0:
        raise FileNotFoundError(f"Aucun fichier Parquet sous {uri}")
    return pd.read_parquet(tmp)


def matrice_sentinelle(df: pd.DataFrame) -> pd.DataFrame:
    X = df[VARIABLES].copy()
    for col in VARIABLES:
        if X[col].isna().any():
            X[f"{col}_manquant"] = X[col].isna().astype(int)
            X[col] = X[col].fillna(SENTINELLE)
    return X


def joli(nom: str) -> str:
    base = nom.replace("_manquant", "")
    suffixe = " (absent)" if nom.endswith("_manquant") else ""
    return ETIQUETTES.get(base, base) + suffixe


def main() -> int:
    p = argparse.ArgumentParser(description="Évaluation et SHAP — SoundLab")
    p.add_argument("--features", required=True)
    p.add_argument("--sortie", required=True, help="s3://<models>/evaluation/")
    p.add_argument("--rapport", required=True)
    p.add_argument("--local", default="./rapports")
    p.add_argument("--region", default=os.environ.get("AWS_REGION", "eu-north-1"))
    args = p.parse_args()

    s3 = boto3.client("s3", region_name=args.region, config=CONFIG_AWS)
    sortie = Path(args.local)
    sortie.mkdir(parents=True, exist_ok=True)

    journal("Chargement de la table de features")
    df = charger_depuis_s3(s3, args.features)
    y = df["is_hit"]
    groupes = df["artist"]
    X = matrice_sentinelle(df)
    journal(f"{len(df):,} pistes · {X.shape[1]} colonnes")

    # =========================================================================
    #  1. Séparation identique à celle de l'ablation
    # =========================================================================
    gss = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=GRAINE)
    i_tr, i_te = next(gss.split(X, y, groups=groupes))
    X_tr, X_te = X.iloc[i_tr], X.iloc[i_te]
    y_tr, y_te = y.iloc[i_tr], y.iloc[i_te]
    journal(f"Entraînement {len(i_tr):,} · test {len(i_te):,} "
            f"(aucun artiste des deux côtés)")

    modele = RandomForestClassifier(
        n_estimators=300, min_samples_leaf=5,
        class_weight="balanced_subsample", n_jobs=-1, random_state=GRAINE)
    modele.fit(X_tr, y_tr)
    proba = modele.predict_proba(X_te)[:, 1]

    # =========================================================================
    #  2. Métriques et seuil d'exploitation
    # =========================================================================
    auc = float(roc_auc_score(y_te, proba))
    ap = float(average_precision_score(y_te, proba))
    taux_base = float(y_te.mean())

    fpr, tpr, seuils_roc = roc_curve(y_te, proba)
    i = int(np.searchsorted(tpr, RAPPEL_CIBLE, side="left"))
    seuil = float(seuils_roc[min(i, len(seuils_roc) - 1)])
    pred = (proba >= seuil).astype(int)
    prec = float(precision_score(y_te, pred, zero_division=0))
    rapp = float(recall_score(y_te, pred, zero_division=0))

    journal(f"AUC-ROC {auc:.4f} · AUC-PR {ap:.4f} · taux de base {taux_base:.4f}")
    journal(f"Seuil {seuil:.3f} → rappel {rapp:.4f}, précision {prec:.4f} "
            f"(× {prec / taux_base:.2f} sur le hasard)")

    # =========================================================================
    #  3. Courbes d'évaluation
    # =========================================================================
    journal("Graphiques d'évaluation")

    # -- ROC
    fig, ax = plt.subplots(figsize=(6, 5.5))
    ax.plot(fpr, tpr, lw=2, label=f"Modèle retenu (AUC = {auc:.3f})")
    ax.plot([0, 1], [0, 1], "--", lw=1, color="gray", label="Hasard (AUC = 0,500)")
    ax.scatter([fpr[min(i, len(fpr) - 1)]], [tpr[min(i, len(tpr) - 1)]],
               s=70, zorder=5, color="crimson",
               label=f"Seuil d'exploitation ({seuil:.3f})")
    ax.set_xlabel("Taux de faux positifs")
    ax.set_ylabel("Taux de vrais positifs (rappel)")
    ax.set_title("Courbe ROC — jeu de test groupé par artiste")
    ax.legend(loc="lower right", fontsize=9)
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(sortie / "courbe_roc.png", dpi=150)
    plt.close(fig)

    # -- Précision-rappel : plus informative que la ROC sur classes déséquilibrées,
    #    car elle ne récompense pas la bonne détection des négatifs majoritaires.
    pr_p, pr_r, _ = precision_recall_curve(y_te, proba)
    fig, ax = plt.subplots(figsize=(6, 5.5))
    ax.plot(pr_r, pr_p, lw=2, label=f"Modèle retenu (AP = {ap:.3f})")
    ax.axhline(taux_base, ls="--", lw=1, color="gray",
               label=f"Taux de hits ({taux_base:.3f})")
    ax.scatter([rapp], [prec], s=70, zorder=5, color="crimson",
               label=f"Point retenu ({rapp:.2f} / {prec:.2f})")
    ax.set_xlabel("Rappel")
    ax.set_ylabel("Précision")
    ax.set_title("Courbe précision-rappel")
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(sortie / "courbe_precision_rappel.png", dpi=150)
    plt.close(fig)

    # -- Matrice de confusion au seuil d'exploitation
    mc = confusion_matrix(y_te, pred)
    fig, ax = plt.subplots(figsize=(5.5, 5))
    ConfusionMatrixDisplay(mc, display_labels=["Niche", "Hit"]).plot(
        ax=ax, cmap="Blues", colorbar=False, values_format=",d")
    ax.set_title(f"Matrice de confusion — seuil {seuil:.3f}")
    ax.set_xlabel("Prédiction")
    ax.set_ylabel("Réalité")
    fig.tight_layout()
    fig.savefig(sortie / "matrice_confusion.png", dpi=150)
    plt.close(fig)

    # =========================================================================
    #  4. Valeurs SHAP
    #
    #  Calculées sur un échantillon aléatoire de 3 000 observations du jeu de
    #  test, et non sur sa totalité : le coût de TreeExplainer croît avec le
    #  nombre d'observations multiplié par celui d'arbres, et 3 000 points
    #  suffisent largement à stabiliser des moyennes globales. L'échantillon
    #  est tiré avec une graine fixe, donc reproductible.
    # =========================================================================
    journal("Calcul des valeurs SHAP")
    import shap

    n_ech = min(N_ECHANTILLON_SHAP, len(X_te))
    X_ech = X_te.sample(n=n_ech, random_state=GRAINE)

    explainer = shap.TreeExplainer(modele)
    valeurs = explainer.shap_values(X_ech)
    # Selon la version, la sortie est (n, p) ou (n, p, 2) ou une liste par classe.
    if isinstance(valeurs, list):
        valeurs = valeurs[1]
    valeurs = np.asarray(valeurs)
    if valeurs.ndim == 3:
        valeurs = valeurs[:, :, 1]
    journal(f"{valeurs.shape[0]:,} observations × {valeurs.shape[1]} variables")

    # -- Résumé en essaim : chaque point est une prédiction, la couleur porte
    #    la valeur de la variable. On y lit le sens de l'effet, pas seulement
    #    son ampleur.
    plt.figure(figsize=(9, 7))
    shap.summary_plot(valeurs, X_ech,
                      feature_names=[joli(c) for c in X_ech.columns],
                      max_display=15, show=False)
    plt.title("Contributions SHAP — effet et sens de chaque variable", fontsize=11)
    plt.tight_layout()
    plt.savefig(sortie / "shap_resume.png", dpi=150)
    plt.close()

    # -- Importance globale SHAP (moyenne des valeurs absolues)
    plt.figure(figsize=(9, 6))
    shap.summary_plot(valeurs, X_ech, plot_type="bar",
                      feature_names=[joli(c) for c in X_ech.columns],
                      max_display=15, show=False)
    plt.title("Importance moyenne des variables (|SHAP|)", fontsize=11)
    plt.tight_layout()
    plt.savefig(sortie / "shap_importance.png", dpi=150)
    plt.close()

    # =========================================================================
    #  5. Comparaison des deux classements, et sens des effets
    #
    #  La corrélation entre la valeur d'une variable et sa contribution SHAP
    #  donne le SENS de l'effet : positive, une valeur élevée pousse vers le
    #  hit ; négative, elle en éloigne. C'est l'information qu'une importance
    #  par impureté ne peut pas fournir.
    # =========================================================================
    journal("Comparaison SHAP / importance par impureté")

    shap_moy = np.abs(valeurs).mean(axis=0)
    impurete = modele.feature_importances_

    classement = pd.DataFrame({
        "variable": X_ech.columns,
        "shap_moyen": shap_moy,
        "importance_impurete": impurete,
    })
    classement["rang_shap"] = classement["shap_moyen"].rank(ascending=False).astype(int)
    classement["rang_impurete"] = (
        classement["importance_impurete"].rank(ascending=False).astype(int))
    classement["ecart_de_rang"] = classement["rang_impurete"] - classement["rang_shap"]

    sens = []
    for j, col in enumerate(X_ech.columns):
        v = X_ech[col].to_numpy(dtype=float)
        if np.std(v) == 0 or np.std(valeurs[:, j]) == 0:
            sens.append(np.nan)
        else:
            sens.append(float(np.corrcoef(v, valeurs[:, j])[0, 1]))
    classement["sens_effet"] = sens

    classement = classement.sort_values("shap_moyen", ascending=False)
    classement.to_csv(sortie / "shap_classement.csv", index=False)

    journal("Cinq premières variables :")
    for _, r in classement.head(5).iterrows():
        direction = ("valeur élevée → hit" if r["sens_effet"] > 0.1
                     else "valeur élevée → niche" if r["sens_effet"] < -0.1
                     else "effet non monotone")
        journal(f"    {joli(r['variable']):<44} |SHAP| {r['shap_moyen']:.4f}  "
                f"({direction})")

    # -- Graphiques de dépendance sur les trois premières variables :
    #    ils montrent comment la contribution évolue le long de la plage de
    #    valeurs, et révèlent les effets de seuil ou d'inversion.
    for nom in classement.head(3)["variable"]:
        plt.figure(figsize=(7, 5))
        shap.dependence_plot(
            list(X_ech.columns).index(nom), valeurs, X_ech,
            feature_names=[joli(c) for c in X_ech.columns],
            interaction_index=None, show=False)
        plt.title(f"Dépendance — {joli(nom)}", fontsize=11)
        plt.tight_layout()
        plt.savefig(sortie / f"shap_dependance_{nom}.png", dpi=150)
        plt.close()

    # =========================================================================
    #  6. Publication
    # =========================================================================
    bucket_s, prefixe_s = decouper_s3(args.sortie)
    fichiers = sorted(sortie.glob("*.png")) + sorted(sortie.glob("*.csv"))
    for f in fichiers:
        s3.upload_file(str(f), bucket_s, f"{prefixe_s}/{f.name}")
    journal(f"{len(fichiers)} fichier(s) publié(s) dans s3://{bucket_s}/{prefixe_s}/")

    rapport = {
        "tache": "13_evaluation_shap",
        "horodatage_utc": datetime.now(timezone.utc).isoformat(),
        "separation": "GroupShuffleSplit par artiste, 20 % de test, graine 42",
        "n_entrainement": int(len(i_tr)),
        "n_test": int(len(i_te)),
        "metriques": {
            "auc_roc": round(auc, 4),
            "auc_precision_rappel": round(ap, 4),
            "taux_base_hits": round(taux_base, 4),
            "seuil_exploitation": round(seuil, 4),
            "rappel": round(rapp, 4),
            "precision": round(prec, 4),
            "gain_sur_hasard": round(prec / taux_base, 2),
            "matrice_confusion": mc.tolist(),
        },
        "shap": {
            "echantillon": int(n_ech),
            "classement": [
                {
                    "variable": r["variable"],
                    "libelle": joli(r["variable"]),
                    "shap_moyen": round(float(r["shap_moyen"]), 5),
                    "importance_impurete": round(float(r["importance_impurete"]), 5),
                    "rang_shap": int(r["rang_shap"]),
                    "rang_impurete": int(r["rang_impurete"]),
                    "ecart_de_rang": int(r["ecart_de_rang"]),
                    "sens_effet": (None if pd.isna(r["sens_effet"])
                                   else round(float(r["sens_effet"]), 4)),
                }
                for _, r in classement.iterrows()
            ],
        },
        "graphiques": [f.name for f in fichiers],
        "statut": "SUCCES",
    }

    bucket_r, prefixe_r = decouper_s3(args.rapport)
    cle = f"{prefixe_r}/rapport_shap_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.json"
    s3.put_object(
        Bucket=bucket_r, Key=cle,
        Body=json.dumps(rapport, indent=2, ensure_ascii=False, default=str).encode(),
        ContentType="application/json",
    )

    print("", flush=True)
    journal("SYNTHÈSE")
    print(f"      AUC-ROC             {auc:.4f}", flush=True)
    print(f"      AUC précision-rappel {ap:.4f}  (taux de base {taux_base:.4f})", flush=True)
    print(f"      Seuil retenu        {seuil:.4f}", flush=True)
    print(f"      Rappel / précision  {rapp:.4f} / {prec:.4f} "
          f"(× {prec / taux_base:.2f})", flush=True)
    print(f"      Graphiques          {sortie.resolve()}", flush=True)
    journal(f"Rapport : s3://{bucket_r}/{cle}")
    journal("Tâche 13 terminée")
    return 0


if __name__ == "__main__":
    sys.exit(main())
