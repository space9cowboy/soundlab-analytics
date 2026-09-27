import sys, time
t_app = time.time()
from pyspark.sql import SparkSession, functions as F, types as T

brut, debut, fin, sortie, palier = sys.argv[1:6]
spark = SparkSession.builder.appName(f"3v_mc_reel_{palier}").getOrCreate()
t_session = time.time()
spark.conf.set("spark.sql.session.timeZone", "UTC")

schema = T.StructType([
    T.StructField("user_id", T.StringType()),
    T.StructField("timestamp", T.LongType()),
    T.StructField("recording_msid", T.StringType()),
    T.StructField("_rebut", T.StringType()),
])
df = (spark.read.schema(schema)
      .option("basePath", brut)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_rebut")
      .json(brut + "/date=*/part-2663.json.zst")
      .where(F.col("date").between(F.lit(debut).cast("date"), F.lit(fin).cast("date"))))

p = df.agg(
    F.count(F.lit(1)).alias("lignes"),
    F.sum(F.col("_rebut").isNotNull().cast("long")).alias("rebut"),
    F.sum(F.col("recording_msid").isNull().cast("long")).alias("msid_nuls"),
    F.min("date").alias("jour_min"),
    F.max("date").alias("jour_max"),
    F.countDistinct("date").alias("jours"),
).first()
t_profil = time.time()

agg = (df.where(F.col("_rebut").isNull())
       .groupBy("date", "recording_msid")
       .agg(F.count(F.lit(1)).alias("ecoutes"), F.hll_sketch_agg("user_id", 12).alias("sk_auditeurs"))
       .repartition("date"))
agg.write.mode("overwrite").partitionBy("date").parquet(sortie)
t_ecriture = time.time()

relu = spark.read.parquet(sortie).where(F.col("date").between(F.lit(debut).cast("date"), F.lit(fin).cast("date")))
v = relu.agg(F.count(F.lit(1)).alias("groupes"), F.sum("ecoutes").alias("ecoutes")).first()
fichiers = len(relu.inputFiles())
colonnes = sorted(relu.columns)
t_fin = time.time()

print("PALIER", palier, "PERIODE", debut, fin)
print("LIGNES", p["lignes"], "REBUT", p["rebut"], "MSID_NULS", p["msid_nuls"])
print("JOURS", p["jours"], "JOUR_MIN", p["jour_min"], "JOUR_MAX", p["jour_max"])
print("GROUPES_ECRITS", v["groupes"], "ECOUTES_ECRITES", v["ecoutes"], "FICHIERS", fichiers)
print("COLONNES", colonnes)
print("PHASES_S demarrage", round(t_session - t_app, 1), "profil", round(t_profil - t_session, 1),
      "agregation_ecriture", round(t_ecriture - t_profil, 1), "verification", round(t_fin - t_ecriture, 1))
ok = (p["rebut"] == 0 and v["ecoutes"] == p["lignes"] and "user_id" not in colonnes and fichiers == p["jours"])
print("MC_OK" if ok else "MC_ECHEC")
if not ok:
    sys.exit(1)
