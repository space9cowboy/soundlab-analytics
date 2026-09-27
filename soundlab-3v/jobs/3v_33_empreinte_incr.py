# 3v_33 (tache 3.1, D6) : empreinte d'ensemble de la table aplati_incr (lecture seule).
# Lignes, triplets distincts (user_id, timestamp, recording_msid), somme des xxhash64 du triplet,
# repartition par dump. Autotest : on simule un second chargement du plus petit dump sans ecrasement ;
# l'empreinte doit changer, sinon la comparaison avant/apres ne prouverait rien.
import sys
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("3v_33_empreinte_incr").getOrCreate()
col = F.col
d = spark.read.parquet(sys.argv[1]).select("user_id", "timestamp", "recording_msid", "dump").cache()

def empreinte(df):
    r = df.agg(F.count(F.lit(1)).alias("n"),
               F.sum(F.xxhash64("user_id", "timestamp", "recording_msid").cast("decimal(38,0)")).alias("h")).first()
    return r["n"], str(r["h"])

n, h = empreinte(d)
distincts = d.select("user_id", "timestamp", "recording_msid").distinct().count()
par_dump = {r["dump"]: r["count"] for r in d.groupBy("dump").count().collect()}
print("EMPREINTE lignes", n, "triplets_distincts", distincts, "somme_xxhash64", h, "par_dump", dict(sorted(par_dump.items())), flush=True)
petit = min(par_dump, key=par_dump.get)
n2, h2 = empreinte(d.unionByName(d.where(col("dump") == petit)))
detecte = n2 != n and h2 != h
print("AUTOTEST double chargement simule du dump", petit, "lignes", n2, "somme", h2,
      "DETECTION_OK" if detecte else "DETECTION_ECHEC", flush=True)
print("EMPREINTE_OK" if n == distincts and detecte else "EMPREINTE_ECHEC")
sys.exit(0 if n == distincts and detecte else 1)
