# 3v_31 (tache 3.1, mesure) : doublons dans la zone de transit et recouvrement avec la table aplatie.
# Lecture seule, aucune ecriture. Args : transit_motif aplati attendu_total
import sys, time
t_app = time.time()
from pyspark.sql import SparkSession, functions as F

transit, aplati, attendu = sys.argv[1], sys.argv[2], int(sys.argv[3])
spark = SparkSession.builder.appName("3v_31_mesure_recouvrement").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
col = F.col
j = lambda c: F.get_json_object("value", "$." + c)
t = (spark.read.text(transit)
     .select(F.regexp_extract(F.input_file_name(), r"dump=(\d+)", 1).alias("dump"),
             j("user_id").alias("user_id"), j("timestamp").cast("long").alias("timestamp"),
             j("recording_msid").alias("recording_msid"))
     .withColumn("mois", F.date_format(F.to_date(F.from_unixtime("timestamp")), "yyyy-MM"))
     .cache())
par_dump = {r["dump"]: r["count"] for r in t.groupBy("dump").count().collect()}
total = sum(par_dump.values())
nuls = t.where(col("user_id").isNull() | col("timestamp").isNull() | col("recording_msid").isNull()).count()
print("TRANSIT total", total, "attendu", attendu, "par_dump", par_dump, "cles_nulles", nuls, flush=True)

cle3 = ["user_id", "timestamp", "recording_msid"]
d3 = t.groupBy(cle3).agg(F.count(F.lit(1)).alias("n"), F.countDistinct("dump").alias("dumps")).where(col("n") > 1)
r3 = d3.agg(F.count(F.lit(1)).alias("groupes"), F.sum(col("n") - 1).alias("excedent"),
            F.sum((col("dumps") > 1).cast("long")).alias("inter_dumps")).first()
d2 = t.groupBy("user_id", "timestamp").count().where(col("count") > 1)
r2 = d2.agg(F.count(F.lit(1)).alias("groupes"), F.sum(col("count") - 1).alias("excedent")).first()
print("DOUBLONS_TRANSIT cle(user,ts,msid) groupes", r3["groupes"], "lignes_en_trop", r3["excedent"] or 0,
      "dont_entre_dumps", r3["inter_dumps"] or 0, "| cle(user,ts) groupes", r2["groupes"], "lignes_en_trop", r2["excedent"] or 0, flush=True)

mois_t = sorted(r["mois"] for r in t.select("mois").distinct().collect())
ap = spark.read.parquet(aplati)
mois_ap = sorted(r["mois"] for r in ap.select("mois").distinct().collect())
communs = sorted(set(mois_t) & set(mois_ap))
dans = t.where(col("mois").isin(communs)).count()
print("MOIS transit", len(mois_t), mois_t[0], mois_t[-1], "| aplati", len(mois_ap), mois_ap[0], mois_ap[-1],
      "| communs", len(communs), "ecoutes_transit_dans_mois_communs", dans, flush=True)
a = ap.where(col("mois").isin(communs)).select(*cle3).distinct()
tc = t.where(col("mois").isin(communs))
rec3 = tc.join(a, cle3, "left_semi").count()
rec_sans_uid = tc.join(a.select("timestamp", "recording_msid").distinct(), ["timestamp", "recording_msid"], "left_semi").count()
print("RECOUVREMENT_APLATI cle(user,ts,msid)", rec3, "| cle(ts,msid) sans user_id", rec_sans_uid, flush=True)
ok = total == attendu and nuls == 0
print("MESURE_OK" if ok else "MESURE_ECHEC", "duree_s", round(time.time() - t_app, 1))
if not ok:
    sys.exit(1)
