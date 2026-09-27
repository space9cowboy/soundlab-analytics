#!/usr/bin/env python3
# AN5-b : controle de confusion par l'anciennete. Meme protocole que 3v_50 (importe tel quel).
# Reference : la variante B complete doit redonner l'AUC mesuree par 3v_50, sinon arret.
import argparse, importlib.util, json, sys
from pathlib import Path
import numpy as np

ICI = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("an5", ICI / "3v_50_reentrainement_cible_lb.py")
an5 = importlib.util.module_from_spec(spec); spec.loader.exec_module(an5)
A, C = an5.AUDIO, an5.CONTEXTE
CONFIGS = [("B_complet", A + C),
           ("B_sans_anciennete", A + [c for c in C if c != "anciennete"]),
           ("B_sans_catalogue", A + [c for c in C if c != "artist_nb_titres_hors_piste"]),
           ("B_sans_anciennete_ni_catalogue", A + ["duration_min"]),
           ("anciennete_seule", ["anciennete"])]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--features", required=True)
    p.add_argument("--cible", required=True)
    p.add_argument("--rapport", required=True)
    p.add_argument("--reference-lb", type=float, default=0.7179)
    p.add_argument("--reference-kaggle", type=float, default=0.6624)
    p.add_argument("--tolerance", type=float, default=0.0005)
    x = p.parse_args()
    df = an5.charger(x.features).drop_duplicates("track_id")
    ci = an5.charger(x.cible)[["track_id", "is_hit_lb_communs", "lb_plays"]]
    c = df.merge(ci.dropna(subset=["is_hit_lb_communs"]), on="track_id", how="inner").reset_index(drop=True)
    yk, yl, g = c["is_hit"].astype(int), c["is_hit_lb_communs"].astype(int), c["artist"]
    q = np.nanquantile(c["anciennete"].astype(float), [0, .1, .5, .9, 1])
    print("COMMUNS", len(c), "ANCIENNETE min/p10/p50/p90/max", [round(float(v), 1) for v in q],
          "manquantes", int(c["anciennete"].isna().sum()), flush=True)
    rs = lambda a, b: round(float(c[a].rank().corr(c[b].rank())), 4)
    print("SPEARMAN anciennete_vs_lb_plays", rs("anciennete", "lb_plays"),
          "anciennete_vs_total_plays", rs("anciennete", "total_plays"),
          "catalogue_vs_lb_plays", rs("artist_nb_titres_hors_piste", "lb_plays"),
          "catalogue_vs_total_plays", rs("artist_nb_titres_hors_piste", "total_plays"), flush=True)
    r = {"tache": "3v_AN5b", "resultats": {}}
    for nom, cols in CONFIGS:
        X = an5.preparer(c, cols)
        for nc, y in (("kaggle", yk), ("listenbrainz", yl)):
            aucs, _ = an5.validation_croisee(X, y, g)
            m, s = round(float(np.mean(aucs)), 4), round(float(np.std(aucs)), 4)
            r["resultats"][f"{nom}__{nc}"] = {"auc_moyen": m, "auc_ecart_type": s, "plis": [round(v, 4) for v in aucs]}
            print("CV", f"{nom}__{nc}", "auc", m, "+-", s, flush=True)
    ref_l = r["resultats"]["B_complet__listenbrainz"]["auc_moyen"]
    ref_k = r["resultats"]["B_complet__kaggle"]["auc_moyen"]
    ok = abs(ref_l - x.reference_lb) <= x.tolerance and abs(ref_k - x.reference_kaggle) <= x.tolerance
    print("REFERENCE B_complet lb", ref_l, "attendu", x.reference_lb, "kaggle", ref_k, "attendu", x.reference_kaggle,
          "REPRODUITE" if ok else "NON_REPRODUITE", flush=True)
    r["statut"] = "AN5B_OK" if ok else "AN5B_ECHEC"
    Path(x.rapport).write_text(json.dumps(r, indent=2, ensure_ascii=False))
    print(r["statut"], flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
