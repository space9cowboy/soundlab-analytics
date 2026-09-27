import os, sys, re, shutil, subprocess, tempfile, math
import numpy as np, pandas as pd
ICI = os.path.dirname(os.path.abspath(__file__))
J40 = os.path.join(ICI, "..", "jobs", "3v_40_jointure_kaggle_lb.py")
J42 = os.path.join(ICI, "..", "jobs", "3v_42_auditeurs_representativite.py")
import importlib.util
sp_ = importlib.util.spec_from_file_location("j42", J42); j42 = importlib.util.module_from_spec(sp_); sp_.loader.exec_module(j42)
from pyspark.sql import SparkSession
d = tempfile.mkdtemp(prefix="t24_"); cur = d + "/cur"
rng = np.random.default_rng(3)
NK = 40
kag = pd.DataFrame({"track_id": ["K%02d" % i for i in range(NK)], "name": ["Song %d" % i for i in range(NK)],
                    "artist": ["Artist %d" % (i % 8) for i in range(NK)],
                    "spotify_id": [None] * NK, "date_ingestion": ["2026-09-10"] * NK})
kag["year"] = [0 if i % 9 == 0 else 1970 + (i * 7) % 45 for i in range(NK)]
kag["genre"] = [None if i % 3 == 0 else ["Rock", "Pop", "Jazz"][i % 3 - 1] for i in range(NK)]
kag["total_plays"] = rng.integers(1, 3000, NK); kag["unique_listeners"] = (kag["total_plays"] // 3 + 1)
thr = j42.quantile(list(kag["total_plays"].iloc[:36]), .75)
kag["is_hit"] = (kag["total_plays"] >= thr).astype(int)
# ecoutes : 30 jetons, titres Kaggle 0..35 (36..39 jamais ecoutes), + bruit hors catalogue
rows = []
for i in range(36):
    for _ in range(int(rng.integers(1, 40))):
        u = "u%02d" % min(int(rng.exponential(6)), 29)
        rows.append((u, "%d-%02d" % (int(rng.integers(2003, 2017)), int(rng.integers(1, 13))), "msid%d" % i, "Artist %d" % (i % 8), "Song %d" % i))
for _ in range(300):
    rows.append(("u%02d" % int(rng.integers(0, 30)), "2012-05", "bruit", "Nobody", "Nothing"))
ec = pd.DataFrame(rows, columns=["user_id", "mois", "recording_msid", "artist_name", "track_name"])
spark = SparkSession.builder.master("local[2]").appName("t").getOrCreate()
sdf = spark.createDataFrame(ec.assign(spotify_id=None, spotify_track_uri=None).astype({"spotify_id": "object", "spotify_track_uri": "object"}),
                            "user_id string, mois string, recording_msid string, artist_name string, track_name string, spotify_id string, spotify_track_uri string")
sdf.write.partitionBy("mois").parquet(d + "/aplati")
spark.createDataFrame(kag[["track_id", "name", "artist", "spotify_id", "date_ingestion"]],
                      "track_id string, name string, artist string, spotify_id string, date_ingestion string").write.parquet(cur + "/music_info")
lab = kag.iloc[:36][["track_id", "artist", "year", "genre", "total_plays", "unique_listeners", "is_hit"]]
spark.createDataFrame(lab, "track_id string, artist string, year long, genre string, total_plays long, unique_listeners long, is_hit int").write.parquet(cur + "/songs_features_labeled")
spark.createDataFrame([("M1", "Other", "X", "otherx")], "recording_mbid string, artist_credit_name string, recording_name string, combined_lookup string").write.parquet(cur + "/trois_v/musicbrainz/mb_canonical_recording")
spark.createDataFrame([("zz", "M1")], "recording_msid string, recording_mbid string").write.parquet(cur + "/trois_v/musicbrainz/mb_correspondance_msid")
spark.stop()
env = dict(os.environ, PYSPARK_SUBMIT_ARGS="--master local[2] pyspark-shell")
tot = len(ec); couv = int((ec["recording_msid"] != "bruit").sum())
p = subprocess.run([sys.executable, J40, d + "/aplati", cur, d + "/an1", str(tot), str(NK)], capture_output=True, text=True, env=env)
assert p.returncode == 0, p.stdout + p.stderr[-2000:]
# attendus independants (pandas)
c = ec[ec["recording_msid"] != "bruit"].copy(); c["track_id"] = ["K%02d" % int(m[4:]) for m in c["recording_msid"]]
pt = c.groupby("track_id").agg(e=("user_id", "size"), a=("user_id", "nunique")).reset_index()
A = lab.merge(pt, on="track_id")
p75v = j42.quantile(list(A["e"]), .75); kv, _ = j42.kappa(list(A["is_hit"] == 1), list(A["e"] >= p75v))
p75a = j42.quantile(list(A["a"]), .75); ka, _ = j42.kappa(list(A["is_hit"] == 1), list(A["a"] >= p75a))
def lancer(kap):
    q = subprocess.run([sys.executable, J42, d + "/aplati", cur, d + "/an1/an1_combos_resolus", d + "/an1", str(tot), str(couv),
                        str(len(A)), str(p75v), str(kap)], capture_output=True, text=True, env=env)
    return q.returncode, q.stdout + q.stderr[-1500:]
rc, out = lancer(round(kv, 4)); print(out[:6000])
f = lambda m: re.search(m, out) is not None
top_u = sorted(ec.groupby("user_id").size())[::-1][:1]
T = [("code 0 et AN24_OK", rc == 0 and f("AN24_OK")),
     ("totaux", f(r"ENTREES ecoutes %d ATTENDU %d couvertes %d ATTENDU %d" % (tot, tot, couv, couv))),
     ("aucune cle ambigue", f(r"CORRESPONDANCE cles_ambigues 0")),
     ("communs", f(r"POPULATIONS communs %d ATTENDU %d trouves %d" % (len(A), len(A), len(pt)))),
     ("kappa auditeurs = pandas", f(r"CIBLE_AUDITEURS seuil %s .*kappa %s" % (re.escape(str(p75a)), re.escape(str(round(ka, 4)))))),
     ("annees : somme = couvertes", sum(int(x) for x in re.findall(r"ANNEE_ECOUTE \d+ ecoutes (\d+)", out)) == couv),
     ("annee 2010 auditeurs = pandas", f(r"ANNEE_ECOUTE 2010 ecoutes %d auditeurs %d" % ((c["mois"].str[:4] == "2010").sum(), c[c["mois"].str[:4] == "2010"]["user_id"].nunique()))),
     ("max jeton toutes ecoutes", f(r"AUDITEURS toutes_ecoutes .*'max': %d" % top_u[0])),
     ("an4 decennies presentes", f(r"AN4 decennie_sortie modalites") and f(r"AN4_PART decennie_sortie inconnue")),
     ("an4 genre absent", f(r"AN4_PART genre \(absent\)")),
     ("an4 artistes", f(r"AN4 artiste spearman_parts")),
     ("ecrit sans jeton", f(r"ECRIT an2_auditeurs_par_titre %d sans_jeton True" % len(pt)))]
rc2, o2 = lancer(round(kv, 4) + 0.01); T.append(("kappa attendu faux -> echec", rc2 == 1 and "AN24_ECHEC" in o2))
T += [("tvd", abs(j42.tvd({"a": .5, "b": .5}, {"a": 1.0}) - .5) < 1e-12), ("decennie", j42.decennie(1987) == "1980s" and j42.decennie(0) == "inconnue")]
for n, ok in T:
    print("OK  " if ok else "ECHEC", n)
print("TESTS", sum(o for _, o in T), "/", len(T)); shutil.rmtree(d)
sys.exit(0 if all(o for _, o in T) else 1)
