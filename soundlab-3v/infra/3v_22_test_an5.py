import os, sys, re, json, subprocess, tempfile, shutil
import numpy as np, pandas as pd
ICI = os.path.dirname(os.path.abspath(__file__))
JOB = os.path.join(ICI, "..", "ml", "3v_50_reentrainement_cible_lb.py")
d = tempfile.mkdtemp(prefix="t_an5_")
rng = np.random.default_rng(0)
n = 1200
AUDIO = ["danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
         "liveness", "valence", "tempo", "time_signature", "duration_ms"]
df = pd.DataFrame({c: rng.random(n) for c in AUDIO})
df["track_id"] = ["T%d" % i for i in range(n)]
df["artist"] = ["A%d" % (i % 300) for i in range(n)]
df["anciennete"] = rng.integers(0, 50, n); df["duration_min"] = df["duration_ms"] * 5
df["artist_nb_titres_hors_piste"] = np.where(rng.random(n) < .1, np.nan, rng.integers(0, 9, n))
df["is_hit"] = (df["danceability"] + .3 * rng.random(n) > np.quantile(df["danceability"] + .3 * rng.random(n), .75)).astype(int)
df.to_parquet(d + "/features.parquet")
ci = df[["track_id"]].iloc[:1000].copy()
ci["is_hit_lb_communs"] = (df["energy"].iloc[:1000] > np.quantile(df["energy"].iloc[:1000], .75)).astype(int)
ci.to_parquet(d + "/cible.parquet")
pk, pl = int(df["is_hit"].iloc[:1000].sum()), int(ci["is_hit_lb_communs"].sum())
def lancer(temoin, communs=1000):
    p = subprocess.run([sys.executable, JOB, "--features", d + "/features.parquet", "--cible", d + "/cible.parquet",
                        "--rapport", d + "/r.json", "--temoin-auc", str(temoin), "--communs-attendu", str(communs),
                        "--positifs-lb-attendu", str(pl), "--positifs-kaggle-attendu", str(pk)], capture_output=True, text=True)
    return p.returncode, p.stdout + p.stderr
rc0, o0 = lancer(0.5)
t = float(re.search(r"TEMOIN B_bloc6 auc ([0-9.]+)", o0).group(1))
rc, out = lancer(t)
print(out)
r = json.load(open(d + "/r.json"))
R = r["resultats"]
T = [("code 0 et AN5_OK", rc == 0 and "AN5_OK" in out),
     ("temoin reproduit", "REPRODUIT" in out and "NON_REPRODUIT" not in out),
     ("temoin rejoue a l'identique (determinisme)", abs(r["temoin_auc"] - t) < 1e-9),
     ("comptes communs OK", re.search(r"COMMUNS titres 1000 .* OK", out) is not None),
     ("4 resultats x 5 plis", len(R) == 4 and all(len(v["auc_plis"]) == 5 for v in R.values())),
     ("kaggle suit danceability (A > 0.75)", R["A_audio_seul__kaggle"]["auc_moyen"] > .75),
     ("lb suit energy (A > 0.9)", R["A_audio_seul__listenbrainz"]["auc_moyen"] > .9),
     ("taux de base exacts par cible", R["B_audio_contexte__listenbrainz"]["taux_base"] == round(pl / 1000, 4)
      and R["B_audio_contexte__kaggle"]["taux_base"] == round(pk / 1000, 4) and pl != pk),
     ("lb et kaggle distincts", R["A_audio_seul__listenbrainz"]["auc_plis"] != R["A_audio_seul__kaggle"]["auc_plis"])]
rc2, o2 = lancer(round(t + .01, 4)); T.append(("temoin faux -> echec", rc2 == 1 and "NON_REPRODUIT" in o2 and "AN5_ECHEC" in o2))
rc3, o3 = lancer(t, 999); T.append(("communs faux -> echec", rc3 == 1 and "ECART" in o3))
for nm, ok in T:
    print("OK  " if ok else "ECHEC", nm)
print("TESTS", sum(o for _, o in T), "/", len(T))
shutil.rmtree(d)
sys.exit(0 if all(o for _, o in T) else 1)
