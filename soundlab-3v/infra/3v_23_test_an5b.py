import os, sys, re, subprocess, tempfile, shutil
import numpy as np, pandas as pd
ICI = os.path.dirname(os.path.abspath(__file__))
JOB = os.path.join(ICI, "..", "ml", "3v_51_controle_contexte.py")
d = tempfile.mkdtemp(prefix="t_an5b_")
rng = np.random.default_rng(1); n = 1500
AUDIO = ["danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
         "liveness", "valence", "tempo", "time_signature", "duration_ms"]
df = pd.DataFrame({c: rng.random(n) for c in AUDIO})
df["track_id"] = ["T%d" % i for i in range(n)]; df["artist"] = ["A%d" % (i % 400) for i in range(n)]
df["anciennete"] = rng.integers(0, 60, n).astype(float); df["duration_min"] = df["duration_ms"] * 5
df["artist_nb_titres_hors_piste"] = rng.integers(0, 9, n).astype(float)
df["total_plays"] = (df["danceability"] * 1000).astype(int)
df["is_hit"] = (df["total_plays"] >= np.quantile(df["total_plays"], .75)).astype(int)
df.to_parquet(d + "/f.parquet")
ci = df[["track_id"]].copy()
ci["lb_plays"] = (df["anciennete"] * 100 + rng.integers(0, 500, n)).astype(int)
ci["is_hit_lb_communs"] = (ci["lb_plays"] >= np.quantile(ci["lb_plays"], .75)).astype(int)
ci.to_parquet(d + "/c.parquet")
def lancer(rl, rk):
    p = subprocess.run([sys.executable, JOB, "--features", d + "/f.parquet", "--cible", d + "/c.parquet", "--rapport",
                        d + "/r.json", "--reference-lb", str(rl), "--reference-kaggle", str(rk)], capture_output=True, text=True)
    return p.returncode, p.stdout + p.stderr
_, o0 = lancer(0, 0)
rl = float(re.search(r"REFERENCE B_complet lb ([0-9.]+)", o0).group(1)); rk = float(re.search(r"kaggle ([0-9.]+) attendu", o0).group(1))
rc, out = lancer(rl, rk); print(out)
auc = lambda k: float(re.search(r"CV %s auc ([0-9.]+)" % k, out).group(1))
T = [("code 0 et AN5B_OK", rc == 0 and "AN5B_OK" in out),
     ("10 lignes CV", len(re.findall(r"^CV ", out, re.M)) == 10),
     ("lb confondue par l'anciennete : B complet eleve", auc("B_complet__listenbrainz") > .85),
     ("lb retombe sans anciennete", auc("B_sans_anciennete__listenbrainz") < .6),
     ("anciennete seule explique lb", auc("anciennete_seule__listenbrainz") > .85),
     ("kaggle insensible a l'anciennete", abs(auc("B_complet__kaggle") - auc("B_sans_anciennete__kaggle")) < .05),
     ("spearman anciennete/lb eleve", float(re.search(r"anciennete_vs_lb_plays ([0-9.-]+)", out).group(1)) > .9)]
rc2, o2 = lancer(round(rl + .01, 4), rk); T.append(("reference fausse -> echec", rc2 == 1 and "NON_REPRODUITE" in o2))
for nm, ok in T:
    print("OK  " if ok else "ECHEC", nm)
print("TESTS", sum(o for _, o in T), "/", len(T)); shutil.rmtree(d)
sys.exit(0 if all(o for _, o in T) else 1)
