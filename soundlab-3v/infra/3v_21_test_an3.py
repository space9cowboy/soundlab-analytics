import os, sys, re, shutil, subprocess, tempfile, random, importlib.util
ICI = os.path.dirname(os.path.abspath(__file__))
JOB = os.path.join(ICI, "..", "jobs", "3v_41_cible_listenbrainz.py")
spec = importlib.util.spec_from_file_location("an3", JOB); an3 = importlib.util.module_from_spec(spec); spec.loader.exec_module(an3)
T = []
# 1. Fonctions statistiques, contre des valeurs calculees a la main
T.append(("quantile lineaire", an3.quantile([1, 2, 3, 4], .75) == 3.25 and an3.quantile([5], .75) == 5))
T.append(("rangs moyens ex aequo", an3.rangs([10, 20, 20, 30]) == [1.0, 2.5, 2.5, 4.0]))
T.append(("spearman parfait", abs(an3.spearman([1, 2, 3, 4], [10, 100, 1000, 10000]) - 1) < 1e-12))
T.append(("spearman inverse", abs(an3.spearman([1, 2, 3, 4], [4, 3, 2, 1]) + 1) < 1e-12))
c = an3.concordance([1, 1, 0, 0, 0, 0, 1, 0], [1, 0, 0, 0, 1, 0, 1, 0])
T.append(("matrice", (c["n11"], c["n10"], c["n01"], c["n00"]) == (2, 1, 1, 4)))
T.append(("kappa 7/15", abs(c["kappa"] - (0.75 - 0.53125) / (1 - 0.53125)) < 1e-12))
T.append(("kappa independant ~0", abs(an3.concordance([1, 1, 0, 0], [1, 0, 1, 0])["kappa"]) < 1e-12))
try:
    import scipy.stats as st
    random.seed(1); x = [random.randint(0, 50) for _ in range(500)]; y = [a + random.randint(0, 80) for a in x]
    T.append(("spearman = scipy (ex aequo)", abs(an3.spearman(x, y) - st.spearmanr(x, y).correlation) < 1e-9))
    import numpy as np
    T.append(("quantile = numpy", abs(an3.quantile(x, .75) - float(np.quantile(x, .75))) < 1e-9))
except ImportError:
    T.append(("scipy absent", False))

# 2. Job complet en Spark local
from pyspark.sql import SparkSession
base = tempfile.mkdtemp(prefix="an3_")
spark = SparkSession.builder.master("local[2]").appName("t").getOrCreate()
lab = [("K%d" % i, p, int(p >= 70)) for i, p in enumerate([10, 20, 30, 40, 50, 60, 70, 80])]
spark.createDataFrame(lab, "track_id string, total_plays long, is_hit int").write.parquet(base + "/cur/songs_features_labeled")
lbp = {"K0": 1, "K1": 2, "K2": 3, "K3": 4, "K4": 5, "K5": 9, "K6": 7, "K7": 8, "N1": 100, "N2": 1}
rows = []
for k, v in lbp.items():
    rows.append(("m" + k, k, v - 1)); rows.append(("m2" + k, k, 1))
rows += [("mx", None, 50), ("my", None, 7)]
spark.createDataFrame(rows, "recording_msid string, track_id string, ecoutes long").write.parquet(base + "/res")
spark.stop()
def lancer(*a):
    p = subprocess.run([sys.executable, JOB, base + "/res", base + "/cur", base + "/out_%s" % "_".join(a)] + list(a),
                       capture_output=True, text=True, env=dict(os.environ, PYSPARK_SUBMIT_ARGS="--master local[2] pyspark-shell"))
    return p.returncode, p.stdout
# total = somme lbp (140) + 57 = 197 ; couvertes = 140 ; communs = 8
rc, out = lancer("197", "140", "8")
print(out)
f = lambda m: re.search(m, out) is not None
T += [("code 0 et AN3_OK", rc == 0 and f("AN3_OK")),
      ("populations", f(r"POPULATIONS etiquetes_kaggle 8 communs 8 ATTENDU 8 trouves_lb 10 nouveaux 2")),
      ("regle kaggle reproduite", f(r"REGLE_KAGGLE seuil_p75 62.5 positifs_reproduits 2 positifs_kaggle 2 .*REPRODUITE")),
      ("seuil LB communs", f(r"CIBLE_LB_A seuil_p75 7.25 positifs 2 ")),
      ("concordance communs", f(r"'n11': 1, 'n10': 1, 'n01': 1, 'n00': 5")),
      ("kappa communs 0.3333", f(r"CONCORDANCE_A .*'kappa': 0.3333")),
      ("spearman 0.9286", f(r"SPEARMAN_A total_plays_vs_lb_plays 0.9286")),
      ("nouveaux positifs 1 sur 2", f(r"nouveaux_positifs 1 sur 2")),
      ("table ecrite 10", f(r"ECRIT an3_cible_par_titre 10"))]
rc2, out2 = lancer("198", "140", "8"); T.append(("total faux -> echec", rc2 == 1 and "AN3_ECHEC" in out2))
rc3, out3 = lancer("197", "140", "9"); T.append(("communs faux -> echec", rc3 == 1 and "AN3_ECHEC" in out3))
for n, ok in T:
    print("OK  " if ok else "ECHEC", n)
print("TESTS", sum(ok for _, ok in T), "/", len(T))
shutil.rmtree(base)
sys.exit(0 if all(ok for _, ok in T) else 1)
