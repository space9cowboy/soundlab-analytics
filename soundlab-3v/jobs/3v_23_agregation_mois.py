import sys, time, math, re, collections
t_app = time.time()
from pyspark.sql import SparkSession, functions as F, types as T

brut, debut, fin, sortie = sys.argv[1:5]
CIBLE_FICHIER = 256 * 2 ** 20
CIBLE_PARTITION = 128 * 2 ** 20
OCTETS_PAR_GROUPE = 43.8
GROUPES_PAR_LIGNE = 0.72
OCTETS_BRASSES_PAR_LIGNE = 80
LIGNES_DISQUE_DEFAUT = 485194425

spark = SparkSession.builder.appName("3v_agregation_mois").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
t_session = time.time()

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

prof = (df.groupBy(F.date_format("date", "yyyy-MM").alias("mois"))
          .agg(F.count(F.lit(1)).alias("lignes"), F.sum(F.col("_rebut").isNotNull().cast("long")).alias("rebut"),
               F.sum(F.col("recording_msid").isNull().cast("long")).alias("msid_nuls"))
          .collect())
lignes_mois = {r["mois"]: r["lignes"] for r in prof}
total = sum(lignes_mois.values())
rebut = sum(r["rebut"] for r in prof)
msid_nuls = sum(r["msid_nuls"] for r in prof)
t_profil = time.time()

plan = {m: max(1, math.ceil(n * GROUPES_PAR_LIGNE * OCTETS_PAR_GROUPE / CIBLE_FICHIER)) for m, n in lignes_mois.items()}
n_seaux = sum(plan.values())
partitions = max(48, math.ceil(total * OCTETS_BRASSES_PAR_LIGNE / CIBLE_PARTITION))
disque_go = max(20, math.ceil(20 * total / LIGNES_DISQUE_DEFAUT))
spark.conf.set("spark.sql.shuffle.partitions", str(partitions))

plan_df = spark.createDataFrame(sorted(plan.items()), "mois string, n int")
agg = (df.where(F.col("_rebut").isNull())
         .groupBy("date", "recording_msid")
         .agg(F.count(F.lit(1)).alias("ecoutes"), F.hll_sketch_agg("user_id", 12).alias("sk_auditeurs"))
         .withColumn("mois", F.date_format("date", "yyyy-MM"))
         .join(F.broadcast(plan_df), "mois")
         .withColumn("seau", F.pmod(F.hash("recording_msid"), F.col("n")))
         .repartition(n_seaux, "mois", "seau")
         .drop("n", "seau"))
agg.write.mode("overwrite").partitionBy("mois").parquet(sortie)
t_ecriture = time.time()

relu = spark.read.parquet(sortie).where(F.col("mois").isin(list(plan)))
v = relu.agg(F.count(F.lit(1)).alias("groupes"), F.sum("ecoutes").alias("ecoutes"),
             F.countDistinct("mois").alias("mois")).first()
colonnes = sorted(relu.columns)
chemin = spark._jvm.org.apache.hadoop.fs.Path(sortie)
fs = chemin.getFileSystem(spark._jsc.hadoopConfiguration())
it = fs.listFiles(chemin, True)
tailles = collections.defaultdict(list)
while it.hasNext():
    s = it.next()
    p = s.getPath().toString()
    m = re.search(r"mois=(\d{4}-\d{2})/", p)
    if p.endswith(".parquet") and m and m.group(1) in plan:
        tailles[m.group(1)].append(s.getLen())
toutes = [t for l in tailles.values() for t in l]
trop = sum(1 for m in plan if len(tailles.get(m, [])) > plan[m])
t_fin = time.time()

print("DIMENSIONNEMENT LIGNES", total, "MOIS", len(plan), "FICHIERS_PREVUS", n_seaux,
      "SHUFFLE_PARTITIONS", partitions, "DISQUE_REGLE_GO", disque_go)
print("PLAN_2016_12", lignes_mois.get("2016-12"), plan.get("2016-12"))
print("SORTIE GROUPES", v["groupes"], "ECOUTES", v["ecoutes"], "MOIS", v["mois"], "REBUT", rebut, "MSID_NULS", msid_nuls)
print("FICHIERS", len(toutes), "MIN_MO", round(min(toutes) / 1e6, 1), "MAX_MO", round(max(toutes) / 1e6, 1),
      "MOYEN_MO", round(sum(toutes) / len(toutes) / 1e6, 1), "TOTAL_OCTETS", sum(toutes), "MOIS_AU_DELA_DU_PLAN", trop)
print("COLONNES", colonnes)
print("PHASES_S demarrage", round(t_session - t_app, 1), "profil", round(t_profil - t_session, 1),
      "agregation_ecriture", round(t_ecriture - t_profil, 1), "verification", round(t_fin - t_ecriture, 1))
ok = (rebut == 0 and v["ecoutes"] == total and v["mois"] == len(plan) and "user_id" not in colonnes
      and trop == 0 and len(toutes) >= len(plan) and max(toutes) < 2 * CIBLE_FICHIER)
print("AGREG_MOIS_OK" if ok else "AGREG_MOIS_ECHEC")
if not ok:
    sys.exit(1)
