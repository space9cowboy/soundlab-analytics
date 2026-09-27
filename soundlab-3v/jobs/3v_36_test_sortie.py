# 3v_36 (tache 3.3, prerequis de l'option C) : un job EMR Serverless peut-il telecharger un dump ListenBrainz ?
# Le pilote telecharge la tranche de minuit 2675 (3 195 octets) et son empreinte publiee, puis compare.
# Un executeur refait un appel HTTP leger (verification que les executeurs sortent aussi). Aucune ecriture.
import hashlib, sys, time, urllib.request
from pyspark.sql import SparkSession

B = "https://data.metabrainz.org/pub/musicbrainz/listenbrainz/incremental/listenbrainz-dump-2675-20260923-000003-incremental/"
F_ = "listenbrainz-listens-dump-2675-20260923-000003-incremental.tar.zst"

def lire(url):
    with urllib.request.urlopen(url, timeout=20) as r:
        return r.read()

spark = SparkSession.builder.appName("3v_36_test_sortie").getOrCreate()
t0 = time.time()
try:
    donnees = lire(B + F_)
    publie = lire(B + F_ + ".sha256").decode().split()[0]
    calcule = hashlib.sha256(donnees).hexdigest()
    pilote = f"octets {len(donnees)} sha_publie {publie[:12]} sha_calcule {calcule[:12]} {'IDENTIQUES' if publie == calcule else 'DIFFERENTS'}"
    ok_pilote = len(donnees) == 3195 and publie == calcule
except Exception as e:
    pilote, ok_pilote = f"ERREUR {type(e).__name__}: {e}", False
print("SORTIE_PILOTE", pilote, "duree_s", round(time.time() - t0, 1), flush=True)

def sonde(_):
    try:
        return [f"OK {len(lire(B + F_ + '.sha256'))} octets"]
    except Exception as e:
        return [f"ERREUR {type(e).__name__}: {e}"]

exe = spark.sparkContext.parallelize([0], 1).mapPartitions(sonde).collect()[0]
print("SORTIE_EXECUTEUR", exe, flush=True)
ok = ok_pilote and exe.startswith("OK")
print("SORTIE_INTERNET_OK" if ok else "SORTIE_INTERNET_ECHEC")
sys.exit(0 if ok else 1)
