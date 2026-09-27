import sys
from pyspark.sql import SparkSession, functions as F

sortie = sys.argv[1]
spark = SparkSession.builder.appName("3v_test_hll").getOrCreate()
print("SPARK_VERSION", spark.version)
noms = ["hll_sketch_agg", "hll_union_agg", "hll_sketch_estimate"]
print("HLL_FONCTIONS", all(hasattr(F, n) for n in noms))

df = spark.range(0, 2000000).select(
    (F.col("id") % 50).alias("titre"),
    (F.col("id") / 1000000).cast("int").alias("jour"),
    (F.col("id") % 300000).alias("auditeur"),
)
jour = df.groupBy("titre", "jour").agg(F.hll_sketch_agg("auditeur", 12).alias("sk"))
jour.write.mode("overwrite").parquet(sortie)
relu = spark.read.parquet(sortie)
print("TYPE_SK", relu.schema["sk"].dataType.simpleString())

fusion = relu.groupBy("titre").agg(F.hll_sketch_estimate(F.hll_union_agg("sk")).alias("est"))
naif = relu.groupBy("titre").agg(F.sum(F.hll_sketch_estimate("sk")).alias("somme_naive"))
exact = df.groupBy("titre").agg(F.countDistinct("auditeur").alias("exact"))
j = fusion.join(exact, "titre").join(naif, "titre").withColumn(
    "err", F.abs(F.col("est") - F.col("exact")) / F.col("exact"))
s = j.agg(F.count("*").alias("n"), F.min("exact").alias("mn"), F.max("exact").alias("mx"),
          F.avg("somme_naive").alias("naive"), F.max("err").alias("emax"),
          F.avg("err").alias("emoy")).first()
print("GROUPES", s["n"])
print("EXACT_MIN_MAX", s["mn"], s["mx"])
print("SOMME_NAIVE_MOYENNE", round(s["naive"]))
print("ERR_MAX", round(s["emax"], 4), "ERR_MOY", round(s["emoy"], 4))
ok = s["n"] == 50 and s["mn"] == 6000 and s["mx"] == 6000 and s["emax"] < 0.05
print("HLL_OK" if ok else "HLL_ECHEC")
if not ok:
    sys.exit(1)
