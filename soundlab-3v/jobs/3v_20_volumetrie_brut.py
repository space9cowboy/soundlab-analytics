import sys, json
from pyspark.sql import SparkSession, functions as F, types as T

brut, man_essai, man_tranche, rapport, attendu = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5])
spark = SparkSession.builder.appName("3v_volumetrie_brut").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
motif = brut + "/date=*/part-2663.json.zst"

schema = T.StructType([
    T.StructField("user_id", T.StringType()),
    T.StructField("user_name", T.StringType()),
    T.StructField("timestamp", T.LongType()),
    T.StructField("recording_msid", T.StringType()),
    T.StructField("track_metadata", T.StructType([
        T.StructField("artist_name", T.StringType()),
        T.StructField("track_name", T.StringType()),
        T.StructField("additional_info", T.StructType([
            T.StructField("recording_mbid", T.StringType()),
            T.StructField("media_player", T.StringType()),
            T.StructField("submission_client", T.StringType()),
        ])),
    ])),
    T.StructField("_rebut", T.StringType()),
])

df = (spark.read.schema(schema)
      .option("basePath", brut)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_rebut")
      .json(motif))
jour_evt = F.to_date(F.timestamp_seconds(F.col("timestamp")))
ai = "track_metadata.additional_info"
g = df.agg(
    F.count(F.lit(1)).alias("lignes"),
    F.countDistinct("user_id").alias("auditeurs"),
    F.approx_count_distinct("recording_msid", 0.01).alias("titres_msid_approx"),
    F.approx_count_distinct(F.concat_ws("\u0001", F.lower("track_metadata.artist_name"), F.lower("track_metadata.track_name")), 0.01).alias("titres_artiste_titre_approx"),
    F.approx_count_distinct(F.col(ai + ".recording_mbid"), 0.01).alias("titres_mbid_approx"),
    F.sum(F.col(ai + ".recording_mbid").isNotNull().cast("long")).alias("lignes_avec_mbid"),
    F.min("timestamp").alias("ts_min"),
    F.max("timestamp").alias("ts_max"),
    F.sum(F.col("_rebut").isNotNull().cast("long")).alias("rebut"),
    F.sum(F.col("user_name").isNotNull().cast("long")).alias("user_name_presents"),
    F.sum((F.col(ai + ".media_player").isNotNull() | F.col(ai + ".submission_client").isNotNull()).cast("long")).alias("champs_supprimes_presents"),
    F.sum(F.when(F.col("user_id").rlike("^[0-9a-f]{32}$"), 0).otherwise(1)).alias("jetons_invalides"),
    F.sum(F.when(jour_evt == F.col("date"), 0).otherwise(1)).alias("hors_partition"),
).first().asDict()

txt = spark.read.option("basePath", brut).text(motif)
par_mois = txt.groupBy(F.date_format("date", "yyyy-MM").alias("mois")).agg(F.count(F.lit(1)).alias("lignes_s3"))
man = (spark.read.option("multiLine", True).json([man_essai, man_tranche])
       .select(F.explode("mois").alias("m"))
       .select(F.col("m.mois").alias("mois"), F.col("m.ecoutes").alias("lignes_manifeste")))
cmp = par_mois.join(man, "mois", "full_outer")
ecarts = cmp.where(F.col("lignes_s3").isNull() | F.col("lignes_manifeste").isNull() | (F.col("lignes_s3") != F.col("lignes_manifeste")))
n_ecarts = ecarts.count()
n_mois = cmp.count()
total_texte = par_mois.agg(F.sum("lignes_s3")).first()[0]

g["periode_debut"] = str(spark.sql(f"select to_date(timestamp_seconds({g['ts_min']}))").first()[0])
g["periode_fin"] = str(spark.sql(f"select to_date(timestamp_seconds({g['ts_max']}))").first()[0])
g["lignes_texte"] = total_texte
g["mois_compares"] = n_mois
g["mois_en_ecart"] = n_ecarts
g["attendu_manifeste"] = attendu
for k in ["lignes", "lignes_texte", "attendu_manifeste", "auditeurs", "titres_msid_approx", "titres_artiste_titre_approx",
          "titres_mbid_approx", "lignes_avec_mbid", "periode_debut", "periode_fin", "mois_compares", "mois_en_ecart",
          "rebut", "user_name_presents", "champs_supprimes_presents", "jetons_invalides", "hors_partition"]:
    print(k.upper(), g[k])
for r in ecarts.limit(5).collect():
    print("  ECART", r["mois"], r["lignes_s3"], r["lignes_manifeste"])
spark.createDataFrame([json.dumps(g, default=str)], "string").coalesce(1).write.mode("overwrite").text(rapport)
ok = (g["lignes"] == attendu == g["lignes_texte"] and n_ecarts == 0 and g["rebut"] == 0 and g["user_name_presents"] == 0
      and g["champs_supprimes_presents"] == 0 and g["jetons_invalides"] == 0 and g["hors_partition"] == 0)
print("VOLUMETRIE_OK" if ok else "VOLUMETRIE_ECHEC")
if not ok:
    sys.exit(1)
