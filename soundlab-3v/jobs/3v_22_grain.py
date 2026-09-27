import sys, time
from pyspark.sql import SparkSession, functions as F

src = sys.argv[1]
spark = SparkSession.builder.appName("3v_grain").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
d = spark.read.parquet(src)
grains = [
    ("jour", F.col("date")),
    ("semaine", F.date_trunc("week", F.col("date")).cast("date")),
    ("mois", F.date_trunc("month", F.col("date")).cast("date")),
]
for nom, cle in grains:
    t0 = time.time()
    g = (d.groupBy(cle.alias("periode"), "recording_msid")
          .agg(F.sum("ecoutes").alias("ecoutes"), F.hll_union_agg("sk_auditeurs").alias("sk"))
          .select("periode", "ecoutes", F.round(F.hll_sketch_estimate("sk")).cast("long").alias("aud")))
    r = g.agg(
        F.count(F.lit(1)).alias("groupes"),
        F.sum("ecoutes").alias("ecoutes"),
        F.countDistinct("periode").alias("periodes"),
        F.avg((F.col("aud") <= 1).cast("double")).alias("part_1_auditeur"),
        F.avg((F.col("aud") < 5).cast("double")).alias("part_moins_5"),
        F.avg((F.col("aud") < 10).cast("double")).alias("part_moins_10"),
        F.sum(F.when(F.col("aud") >= 5, F.col("ecoutes")).otherwise(0)).alias("ecoutes_si_k5"),
        F.sum(F.when(F.col("periode") >= F.lit("2016-12-01").cast("date"), 1).otherwise(0)).alias("groupes_2016_12"),
    ).first()
    print(f"GRAIN {nom} GROUPES {r['groupes']} ECOUTES {r['ecoutes']} PERIODES {r['periodes']} "
          f"PART_1_AUDITEUR {r['part_1_auditeur']:.4f} PART_MOINS_5 {r['part_moins_5']:.4f} "
          f"PART_MOINS_10 {r['part_moins_10']:.4f} ECOUTES_SI_K5 {r['ecoutes_si_k5']} "
          f"GROUPES_2016_12 {r['groupes_2016_12']} DUREE_S {time.time() - t0:.0f}", flush=True)
