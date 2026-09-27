#!/usr/bin/env python3
# AN5 : meme modele, meme protocole que ml/12 (Bloc 6, fige), deux cibles.
# Cible Kaggle (is_hit) contre cible ListenBrainz (is_hit_lb_communs, tache AN3), sur les titres communs.
# Variables A (13 audio) et B (+ 3 contexte) seulement : les variables d'artiste de C derivent de la cible Kaggle.
# Temoin : la variante B du Bloc 6, sur les 30 459 titres et la separation unique, doit redonner son AUC publiee.
import argparse, json, sys, tempfile
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.model_selection import GroupKFold, GroupShuffleSplit

GRAINE, SENTINELLE = 42, -1.0
AUDIO = ["danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
         "liveness", "valence", "tempo", "time_signature", "duration_ms"]
CONTEXTE = ["anciennete", "duration_min", "artist_nb_titres_hors_piste"]
VARIANTES = [("A_audio_seul", AUDIO), ("B_audio_contexte", AUDIO + CONTEXTE)]
PARAMS = dict(n_estimators=300, max_depth=None, min_samples_leaf=5, class_weight="balanced_subsample",
              n_jobs=-1, random_state=GRAINE)


def journal(m):
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {m}", flush=True)


def charger(uri):
    if not uri.startswith("s3://"):
        return pd.read_parquet(uri)
    import boto3
    b, _, p = uri[5:].rstrip("/").partition("/")
    s3 = boto3.client("s3", region_name="eu-north-1")
    tmp = Path(tempfile.mkdtemp(prefix="an5_"))
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=p + "/"):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".parquet"):
                s3.download_file(b, o["Key"], str(tmp / ("%03d.parquet" % n)))
                n += 1
    if n == 0:
        raise FileNotFoundError(uri)
    return pd.read_parquet(tmp)


def preparer(df, colonnes):
    X = df[colonnes].copy()
    for c in colonnes:
        if X[c].isna().any():
            X[c + "_manquant"] = X[c].isna().astype(int)
            X[c] = X[c].fillna(SENTINELLE)
    return X


def separation_unique(X, y, g):
    i_tr, i_te = next(GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=GRAINE).split(X, y, groups=g))
    m = RandomForestClassifier(**PARAMS).fit(X.iloc[i_tr], y.iloc[i_tr])
    return float(roc_auc_score(y.iloc[i_te], m.predict_proba(X.iloc[i_te])[:, 1]))


def validation_croisee(X, y, g):
    aucs, aps = [], []
    for i_tr, i_te in GroupKFold(n_splits=5).split(X, y, groups=g):
        m = RandomForestClassifier(**PARAMS).fit(X.iloc[i_tr], y.iloc[i_tr])
        p = m.predict_proba(X.iloc[i_te])[:, 1]
        aucs.append(float(roc_auc_score(y.iloc[i_te], p)))
        aps.append(float(average_precision_score(y.iloc[i_te], p)))
    return aucs, aps


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--features", required=True)
    a.add_argument("--cible", required=True)
    a.add_argument("--rapport", required=True)
    a.add_argument("--temoin-auc", type=float, default=0.6585)
    a.add_argument("--tolerance", type=float, default=0.001)
    a.add_argument("--communs-attendu", type=int, default=29315)
    a.add_argument("--positifs-lb-attendu", type=int, default=7330)
    a.add_argument("--positifs-kaggle-attendu", type=int, default=7397)
    x = a.parse_args()

    df = charger(x.features).drop_duplicates("track_id")
    ci = charger(x.cible)[["track_id", "is_hit_lb_communs"]]
    journal(f"features {len(df)} titres, {df['artist'].nunique()} artistes ; cible LB {len(ci)} titres")

    # 1. Temoin : variante B du Bloc 6, population complete, separation unique
    t = separation_unique(preparer(df, AUDIO + CONTEXTE), df["is_hit"], df["artist"])
    temoin_ok = abs(t - x.temoin_auc) <= x.tolerance
    print("TEMOIN B_bloc6 auc", round(t, 4), "publie", x.temoin_auc, "REPRODUIT" if temoin_ok else "NON_REPRODUIT", flush=True)

    # 2. Titres communs, deux cibles
    c = df.merge(ci.dropna(subset=["is_hit_lb_communs"]), on="track_id", how="inner").reset_index(drop=True)
    yk, yl, g = c["is_hit"].astype(int), c["is_hit_lb_communs"].astype(int), c["artist"]
    comptes_ok = (len(c) == x.communs_attendu and int(yl.sum()) == x.positifs_lb_attendu
                  and int(yk.sum()) == x.positifs_kaggle_attendu)
    print("COMMUNS titres", len(c), "artistes", g.nunique(), "positifs_kaggle", int(yk.sum()),
          "positifs_lb", int(yl.sum()), "OK" if comptes_ok else "ECART", flush=True)

    r = {"tache": "3v_AN5", "horodatage_utc": datetime.now(timezone.utc).isoformat(), "temoin_auc": round(t, 4),
         "communs": len(c), "resultats": {}}
    for nom, cols in VARIANTES:
        X = preparer(c, cols)
        for nc, y in (("kaggle", yk), ("listenbrainz", yl)):
            aucs, aps = validation_croisee(X, y, g)
            su = separation_unique(X, y, g)
            cle = f"{nom}__{nc}"
            r["resultats"][cle] = {"auc_plis": [round(v, 4) for v in aucs], "auc_moyen": round(float(np.mean(aucs)), 4),
                                   "auc_ecart_type": round(float(np.std(aucs)), 4), "auc_pr_moyen": round(float(np.mean(aps)), 4),
                                   "taux_base": round(float(y.mean()), 4), "auc_separation_unique": round(su, 4)}
            v = r["resultats"][cle]
            print("CV", cle, "auc", v["auc_moyen"], "+-", v["auc_ecart_type"], "plis", v["auc_plis"],
                  "auc_pr", v["auc_pr_moyen"], "taux_base", v["taux_base"], "unique", v["auc_separation_unique"], flush=True)

    ok = temoin_ok and comptes_ok
    r["statut"] = "AN5_OK" if ok else "AN5_ECHEC"
    Path(x.rapport).write_text(json.dumps(r, indent=2, ensure_ascii=False))
    print(r["statut"], "rapport", x.rapport, flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
