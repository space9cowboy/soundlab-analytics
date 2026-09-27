import sys, time, math
t_app = time.time()
from pyspark.sql import SparkSession, functions as F

# Tache 4.1 (phase 4, decision du 27/09 : deux origines, un grain).
# Produit les faits d'ecoute ListenBrainz au grain (jour d'ecoute, recording_msid), sans aucun jeton :
#   date, recording_msid, ecoutes (triplets distincts), auditeurs (jetons distincts du groupe), partition mois.
# Mode complet : a partir du dump complet aplati (2002-10 a 2016-12), ecrit <sortie>/origine=complet, une fois.
# Mode incr    : a partir de tous les dumps incrementaux, reconstruit <sortie>/origine=incr a chaque passage :
#   1. dedoublonnage du triplet (user_id, timestamp, recording_msid) entre dumps (mesure 3v_60 : 25 606 lignes) ;
#   2. exclusion des triplets deja presents dans le dump complet (mesure 3v_60 : 293 952), par condensat xxhash64
#      sur les seuls mois couverts par le dump complet.
# L'union des deux origines ne compte donc aucune ecoute deux fois. Limite documentee : pour un meme jour et un meme
# titre present dans les deux origines, la somme des auditeurs est un majorant (un auditeur peut figurer des deux cotes).
# Le jour est recalcule depuis l'horodatage (UTC) dans les deux modes : la colonne date du dump complet est du texte.
# Les jetons ne sortent jamais du job : la zone affinee ne recoit que des comptes (lecon B13).

mode, aplati, aplati_incr, sortie = sys.argv[1:5]
attendu = int(sys.argv[5]) if len(sys.argv) > 5 else None
assert mode in ("complet", "incr"), "mode complet ou incr"
cible = sortie.rstrip("/") + "/origine=" + mode
spark = SparkSession.builder.appName("3v_61_faits_jour_" + mode).getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
K = ["user_id", "timestamp", "recording_msid"]
t_session = time.time()

c = spark.read.parquet(aplati).select(*K, "mois")
if mode == "complet":
    src = c
    lues = src.count()
else:
    i = spark.read.parquet(aplati_incr).select(*K, "mois")
    lues = i.count()
    mois_complet = {r["mois"] for r in c.select("mois").distinct().collect()}
    mois_incr = {r["mois"] for r in i.select("mois").distinct().collect()}
    couverts = sorted(mois_complet & mois_incr)
    i = i.dropDuplicates(K)
    hc = c.where(F.col("mois").isin(couverts)).select(F.xxhash64(*K).alias("h"))
    dedans = i.where(F.col("mois").isin(couverts))
    src = (i.where(~F.col("mois").isin(couverts))
            .unionByName(dedans.withColumn("h", F.xxhash64(*K)).join(hc, "h", "left_anti").drop("h")))
partitions = max(48, math.ceil(lues * 80 / (128 * 2 ** 20)))
spark.conf.set("spark.sql.shuffle.partitions", str(partitions))

jour = F.to_date(F.from_unixtime("timestamp"))
par_auditeur = (src.groupBy(jour.alias("date"), "recording_msid", "user_id")
                   .agg(F.countDistinct("timestamp").alias("n")))
faits = (par_auditeur.groupBy("date", "recording_msid")
                     .agg(F.sum("n").alias("ecoutes"), F.count(F.lit(1)).alias("auditeurs"))
                     .withColumn("mois", F.date_format("date", "yyyy-MM")))
(faits.repartition("mois").write.mode("overwrite").option("maxRecordsPerFile", 8000000)
      .partitionBy("mois").parquet(cible))
t_ecriture = time.time()

relu = spark.read.parquet(cible)
v = relu.agg(F.count(F.lit(1)).alias("groupes"), F.sum("ecoutes").alias("ecoutes"), F.sum("auditeurs").alias("auditeurs"),
             F.countDistinct("mois").alias("mois"), F.min("date").alias("dmin"), F.max("date").alias("dmax"),
             F.sum((F.col("auditeurs") < 1).cast("long")).alias("aud_nuls"),
             F.sum((F.col("ecoutes") < F.col("auditeurs")).cast("long")).alias("incoherents"),
             F.sum(F.col("recording_msid").isNull().cast("long")).alias("msid_nuls"),
             F.sum((F.date_format("date", "yyyy-MM") != F.col("mois")).cast("long")).alias("mois_faux"),
             F.sum((F.col("auditeurs") == 1).cast("long")).alias("un_auditeur")).first()
colonnes = sorted(relu.columns)
fichiers = len([f for f in relu.inputFiles() if f.endswith(".parquet")])
t_fin = time.time()

ecoutes = v["ecoutes"] or 0
print("MODE", mode, "CIBLE", cible)
print("LUES", lues, "ECOUTES_DISTINCTES", ecoutes, "RETIREES", lues - ecoutes, "ATTENDU", attendu)
print("SORTIE GROUPES", v["groupes"], "AUDITEURS_SOMME", v["auditeurs"], "MOIS", v["mois"],
      "DATES", v["dmin"], v["dmax"], "FICHIERS", fichiers, "SHUFFLE_PARTITIONS", partitions)
print("CONTROLES aud_nuls", v["aud_nuls"], "ecoutes<auditeurs", v["incoherents"], "msid_nuls", v["msid_nuls"],
      "mois_faux", v["mois_faux"], "groupes_a_un_auditeur", v["un_auditeur"])
print("COLONNES", colonnes)
print("PHASES_S demarrage", round(t_session - t_app, 1), "calcul_ecriture", round(t_ecriture - t_session, 1),
      "verification", round(t_fin - t_ecriture, 1))
ok = (colonnes == ["auditeurs", "date", "ecoutes", "mois", "recording_msid"]
      and (v["groupes"] or 0) > 0 and 0 < ecoutes <= lues
      and v["aud_nuls"] == 0 and v["incoherents"] == 0 and v["mois_faux"] == 0
      and (attendu is None or ecoutes == attendu))
print("FAITS_OK" if ok else "FAITS_ECHEC")
if not ok:
    sys.exit(1)
