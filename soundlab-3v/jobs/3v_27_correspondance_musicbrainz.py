import sys, time
t_app = time.time()
from pyspark.sql import SparkSession, functions as F, Window

aplati, mb, tmp, sortie, base, attendu = sys.argv[1:7]
attendu = int(attendu)
spark = SparkSession.builder.appName("3v_correspondance_musicbrainz").getOrCreate()
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
col = F.col
TRANCHES = [("2002-01", "2008-12")] + [("%d-01" % a, "%d-12" % a) for a in range(2009, 2017)]

def cle_ascii(texte):
    return F.when(texte.rlike("^[\\x00-\\x7F]*$"),
                  F.nullif(F.regexp_replace(F.lower(texte), "[^a-z0-9_]", ""), F.lit("")))

# 1. Combinaisons agregees par tranche (shuffle borne), ecrites en zone intermediaire non personnelle
t0 = time.time()
src = spark.read.parquet(aplati)
for i, (a, b) in enumerate(TRANCHES):
    (src.where(col("mois").between(a, b))
        .select("recording_msid", col("mbid_enregistrement").alias("mbid"), col("lastfm_track_mbid").alias("lastfm"),
                cle_ascii(F.concat_ws("", col("artist_name"), col("track_name"))).alias("cle"))
        .groupBy("recording_msid", "mbid", "lastfm", "cle").agg(F.count(F.lit(1)).alias("ecoutes"))
        .write.mode("overwrite" if i == 0 else "append").parquet(tmp))
combos = spark.read.parquet(tmp)
tot = combos.agg(F.sum("ecoutes").alias("e"), F.count(F.lit(1)).alias("c")).first()
print("COMBOS lignes", tot["c"], "ECOUTES", tot["e"], "ATTENDU", attendu, "duree_s", round(time.time() - t0, 1), flush=True)

# 2. Referentiel : resolution d'un MBID vers l'enregistrement canonique
canon = spark.read.parquet(mb + "/mb_canonical_recording")
redir = spark.read.parquet(mb + "/mb_recording_redirect")
res = (canon.select(col("recording_mbid").alias("m"), col("recording_mbid").alias("canon"), F.lit("direct").alias("voie"))
       .unionByName(redir.join(canon.select(col("recording_mbid").alias("recording_mbid_c")),
                               redir.recording_mbid == col("recording_mbid_c"), "left_anti")
                    .select(col("recording_mbid").alias("m"), col("canonical_recording_mbid").alias("canon"),
                            F.lit("redirection").alias("voie"))))

# 3. Hypothese sur combined_lookup, verifiee sur le catalogue (lignes ASCII)
nom = F.concat_ws("", col("artist_credit_name"), col("recording_name"))
h = (canon.where(nom.rlike("^[\\x00-\\x7F]*$"))
     .agg(F.count(F.lit(1)).alias("n"), F.sum((cle_ascii(nom) == col("combined_lookup")).cast("long")).alias("ok")).first())
accord = h["ok"] / h["n"] if h["n"] else 0.0
cle_valide = accord >= 0.99
print("HYPOTHESE_CLE lignes_ascii", h["n"], "accord", h["ok"], "taux", round(accord, 5),
      "RETENUE" if cle_valide else "REJETEE", flush=True)
kmap = (canon.groupBy(col("combined_lookup").alias("cle")).agg(F.count(F.lit(1)).alias("n"),
                                                               F.min("recording_mbid").alias("canonC"))
        .where(col("n") == 1).drop("n"))

# 4. Jointures et cascade mbid -> lastfm -> cle
j = (combos
     .join(res.select(col("m").alias("mbid"), col("canon").alias("canonA"), col("voie").alias("voieA")), "mbid", "left")
     .join(res.select(col("m").alias("lastfm"), col("canon").alias("canonB")), "lastfm", "left"))
if cle_valide:
    j = j.join(kmap, "cle", "left")
else:
    j = j.withColumn("canonC", F.lit(None).cast("string"))
j = (j.withColumn("methode", F.when(col("canonA").isNotNull(), F.concat(F.lit("mbid_"), col("voieA")))
                              .when(col("canonB").isNotNull(), F.lit("lastfm"))
                              .when(col("canonC").isNotNull(), F.lit("cle_artiste_titre")).otherwise(F.lit("aucune")))
      .withColumn("canon", F.coalesce("canonA", "canonB", "canonC")))
j.write.mode("overwrite").parquet(tmp + "_resolu")
r = spark.read.parquet(tmp + "_resolu")
e = F.col("ecoutes")
m = r.agg(
    F.sum(e).alias("total"),
    F.sum(F.when(col("mbid").isNotNull(), e).otherwise(0)).alias("mbid_present"),
    F.sum(F.when(col("canonA").isNotNull(), e).otherwise(0)).alias("mbid_resolu"),
    F.sum(F.when(col("voieA") == "redirection", e).otherwise(0)).alias("mbid_redirige"),
    F.sum(F.when(col("lastfm").isNotNull(), e).otherwise(0)).alias("lastfm_present"),
    F.sum(F.when(col("canonB").isNotNull(), e).otherwise(0)).alias("lastfm_resolu"),
    F.sum(F.when(col("cle").isNotNull(), e).otherwise(0)).alias("cle_calculable"),
    F.sum(F.when(col("canonC").isNotNull(), e).otherwise(0)).alias("cle_resolue"),
    F.sum(F.when(col("canon").isNotNull(), e).otherwise(0)).alias("resolu_total")).first()
par_methode = {x["methode"]: x["s"] for x in r.groupBy("methode").agg(F.sum(e).alias("s")).collect()}
pct = lambda a, b: round(100.0 * a / b, 2) if b else 0.0
print("TAUX mbid present", m["mbid_present"], "resolu", m["mbid_resolu"], "(%s %%)" % pct(m["mbid_resolu"], m["mbid_present"]),
      "dont_redirige", m["mbid_redirige"], flush=True)
print("TAUX lastfm present", m["lastfm_present"], "resolu", m["lastfm_resolu"], "(%s %%)" % pct(m["lastfm_resolu"], m["lastfm_present"]), flush=True)
print("TAUX cle calculable", m["cle_calculable"], "resolue", m["cle_resolue"], "(%s %%)" % pct(m["cle_resolue"], m["cle_calculable"]), flush=True)
print("CASCADE", {k: (v, pct(v, m["total"])) for k, v in sorted(par_methode.items())}, flush=True)
print("COUVERTURE_TOTALE", m["resolu_total"], "sur", m["total"], "(%s %%)" % pct(m["resolu_total"], m["total"]), flush=True)

# 5. Table de correspondance msid -> enregistrement canonique (choix majoritaire)
rang = {"mbid_direct": 1, "mbid_redirection": 2, "lastfm": 3, "cle_artiste_titre": 4}
prio = F.create_map(*[x for k, v in rang.items() for x in (F.lit(k), F.lit(v))])
par_msid = (r.where(col("canon").isNotNull())
            .groupBy("recording_msid", "canon").agg(F.sum(e).alias("ecoutes_choix"),
                                                    F.min(prio[col("methode")]).alias("rang")))
w = Window.partitionBy("recording_msid").orderBy(col("ecoutes_choix").desc(), col("rang"), col("canon"))
tot_msid = r.groupBy("recording_msid").agg(F.sum(e).alias("ecoutes_msid"))
inv = F.create_map(*[x for k, v in rang.items() for x in (F.lit(v), F.lit(k))])
corr = (par_msid.withColumn("n", F.row_number().over(w)).where(col("n") == 1)
        .join(tot_msid, "recording_msid")
        .select("recording_msid", col("canon").alias("recording_mbid"), inv[col("rang")].alias("methode"),
                "ecoutes_msid", "ecoutes_choix", F.round(col("ecoutes_choix") / col("ecoutes_msid"), 4).alias("part")))
corr.repartition(8).write.mode("overwrite").parquet(sortie)
relu = spark.read.parquet(sortie)
v = relu.agg(F.count(F.lit(1)).alias("n"), F.countDistinct("recording_msid").alias("d"),
             F.sum("ecoutes_choix").alias("choix"), F.avg("part").alias("part_moy")).first()
hors_ref = relu.join(canon.select("recording_mbid"), "recording_mbid", "left_anti").count()
msid_total = r.select("recording_msid").distinct().count()
if base != "-":
    import boto3
    from botocore.exceptions import ClientError
    glue = boto3.client("glue", region_name="eu-north-1")
    TG = {"StringType()": "string", "LongType()": "bigint", "DoubleType()": "double", "IntegerType()": "int"}
    d = {"Name": "mb_correspondance_msid", "Description": "recording_msid ListenBrainz -> enregistrement canonique MusicBrainz",
         "TableType": "EXTERNAL_TABLE",
         "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE", "projet": "SoundLab", "tache": "3v_2.3"},
         "StorageDescriptor": {"Columns": [{"Name": c.name, "Type": TG.get(str(c.dataType), "string")} for c in relu.schema.fields],
                               "Location": sortie,
                               "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                               "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                               "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                             "Parameters": {"serialization.format": "1"}}, "Compressed": True}}
    try:
        glue.create_table(DatabaseName=base, TableInput=d); etat = "creee"
    except ClientError as err:
        if err.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        glue.update_table(DatabaseName=base, TableInput=d); etat = "mise_a_jour"
else:
    etat = "glue_saute"
print("CORRESPONDANCE lignes", v["n"], "msid_distincts", v["d"], "msid_total", msid_total,
      "msid_resolus_%", pct(v["d"], msid_total), "part_majoritaire_moy", round(v["part_moy"] or 0, 4),
      "HORS_REFERENTIEL", hors_ref, "GLUE", etat, flush=True)
ok = (tot["e"] == attendu and m["total"] == attendu and v["n"] == v["d"] and hors_ref == 0
      and v["choix"] <= m["resolu_total"])
print("CORRESPONDANCE_OK" if ok else "CORRESPONDANCE_ECHEC", "duree_s", round(time.time() - t_app, 1))
if not ok:
    sys.exit(1)
