# 3v_34 (tache 3.2, mesure) : retard entre date d'ecoute et jour de reception d'un dump incremental.
# Lecture seule. Args : aplati_incr dump jour_reception(AAAA-MM-JJ)
import sys
from pyspark.sql import SparkSession, functions as F

aplati, dump, jour = sys.argv[1], sys.argv[2], sys.argv[3]
spark = SparkSession.builder.appName("3v_34_mesure_retard").getOrCreate()
col = F.col
d = (spark.read.parquet(aplati).where(col("dump") == dump)
     .select("date", F.datediff(F.lit(jour).cast("date"), col("date")).alias("retard")).cache())
n = d.count()
tranche = (F.when(col("retard") < 0, "a_negatif").when(col("retard") == 0, "b_0j").when(col("retard") == 1, "c_1j")
           .when(col("retard") <= 7, "d_2-7j").when(col("retard") <= 30, "e_8-30j").when(col("retard") <= 365, "f_31-365j")
           .otherwise("g_plus_365j"))
for r in d.groupBy(tranche.alias("t")).agg(F.count(F.lit(1)).alias("n"), F.countDistinct("date").alias("jours")).orderBy("t").collect():
    print("RETARD", r["t"][2:], "ecoutes", r["n"], "(%.2f %%)" % (100.0 * r["n"] / n), "jours_distincts", r["jours"], flush=True)
q = d.approxQuantile("retard", [0.5, 0.9, 0.99], 0.001)
par_jour = d.groupBy("date").count()
s = par_jour.agg(F.count(F.lit(1)).alias("j"), F.expr("percentile_approx(count, 0.5)").alias("med"),
                 F.max("count").alias("mx"), F.sum((col("count") < 100).cast("long")).alias("petits")).first()
print("TOTAL", n, "dump", dump, "reception", jour, "retard_jours P50/P90/P99", q, flush=True)
print("PARTITIONS_JOUR", s["j"], "ecoutes_par_jour mediane", s["med"], "max", s["mx"], "jours_de_moins_de_100_ecoutes", s["petits"], flush=True)
