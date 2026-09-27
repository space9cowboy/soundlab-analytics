import sys, time
t_app = time.time()
from pyspark.sql import SparkSession, functions as F

resolu, mb = sys.argv[1:3]
spark = SparkSession.builder.appName("3v_precision_correspondance").getOrCreate()
col, e = F.col, F.col("ecoutes")
r = spark.read.parquet(resolu)

def accord(a, b, nom):
    x = r.where(col(a).isNotNull() & col(b).isNotNull()).agg(
        F.count(F.lit(1)).alias("combos"), F.sum(e).alias("ecoutes"),
        F.sum(F.when(col(a) == col(b), e).otherwise(0)).alias("accord")).first()
    t = round(100.0 * x["accord"] / x["ecoutes"], 2) if x["ecoutes"] else None
    print("ACCORD", nom, "combos", x["combos"], "ecoutes", x["ecoutes"], "identiques", x["accord"], "taux_%", t, flush=True)
    return x["ecoutes"] or 0

n = accord("canonA", "canonC", "mbid_vs_cle")
accord("canonA", "canonB", "mbid_vs_lastfm")
accord("canonB", "canonC", "lastfm_vs_cle")

canon = spark.read.parquet(mb + "/mb_canonical_recording")
rel = (canon.select(col("release_mbid").alias("x"))
       .unionByName(spark.read.parquet(mb + "/mb_release_redirect").select(col("release_mbid").alias("x")))
       .unionByName(spark.read.parquet(mb + "/mb_release_redirect").select(col("canonical_release_mbid").alias("x")))
       .distinct())
for champ, canon_col, nom in (("mbid", "canonA", "mbid"), ("lastfm", "canonB", "lastfm")):
    nr = r.where(col(champ).isNotNull() & col(canon_col).isNull())
    tot = nr.agg(F.sum(e).alias("s"), F.countDistinct(champ).alias("d")).first()
    est_parution = (nr.select(champ, "ecoutes").join(rel, nr[champ] == rel.x, "left_semi")
                    .agg(F.sum(e).alias("s"), F.countDistinct(champ).alias("d")).first())
    print("NON_RESOLUS", nom, "ecoutes", tot["s"], "identifiants", tot["d"],
          "dont_identifiants_de_parution_ecoutes", est_parution["s"] or 0, "identifiants", est_parution["d"], flush=True)
print("PRECISION_OK" if n > 0 else "PRECISION_ECHEC", "duree_s", round(time.time() - t_app, 1))
if n == 0:
    sys.exit(1)
