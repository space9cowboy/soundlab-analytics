import sys, time
t_app = time.time()
from pyspark.sql import SparkSession, functions as F

# Tache 4.0 (phase 4) : mesure en lecture seule, aucune ecriture.
# Repond a deux questions avant de concevoir l'agregation quotidienne destinee a Redshift :
#  Q1. Une meme ecoute (triplet user_id, timestamp, recording_msid) figure-t-elle dans plusieurs dumps
#      incrementaux ? 3v_24 ne dedoublonne qu'a l'interieur d'un lot.
#  Q2. Les ecoutes anciennes (jour=_ancien, retard > 30 j) des incrementaux figurent-elles deja dans le dump
#      complet aplati ? Si oui, additionner les deux sources les compterait deux fois.
# Temoin de Q2 : le recouvrement sur la paire (timestamp, recording_msid), qui ignore le jeton d'auditeur.
# Paires recouvertes nombreuses mais triplets nuls = jetons differents entre les deux sources, et non absence
# de doublons : le job le signale au lieu de conclure.
# Les cles sont reduites a des condensats xxhash64 (8 octets) pour limiter le brassage du dump complet.

incr, complet = sys.argv[1:3]
spark = SparkSession.builder.appName("3v_60_recouvrement_incr").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
K = ["user_id", "timestamp", "recording_msid"]
t_session = time.time()

i = spark.read.parquet(incr).select(*K, "mois", "jour", "dump")
ancien = F.col("jour") == "_ancien"

par_dump = sorted((r["dump"], r["recentes"], r["anciennes"]) for r in
                  i.groupBy("dump").agg(F.sum((~ancien).cast("long")).alias("recentes"),
                                        F.sum(ancien.cast("long")).alias("anciennes")).collect())
total = sum(r + a for _, r, a in par_dump)
recentes = sum(r for _, r, _ in par_dump)
anciennes = total - recentes
uid_invalides = i.where(~F.col("user_id").rlike("^[0-9a-f]{32}$")).count()
mois_anciens = sorted(r["mois"] for r in i.where(ancien).select("mois").distinct().collect())
mois_recents = sorted(r["mois"] for r in i.where(~ancien).select("mois").distinct().collect())

multi = (i.groupBy(F.xxhash64(*K).alias("h"))
          .agg(F.count(F.lit(1)).alias("n"), F.countDistinct("dump").alias("nd"))
          .where(F.col("n") > 1)
          .agg(F.count(F.lit(1)).alias("triplets"), F.sum(F.col("n") - 1).alias("en_trop"),
               F.sum((F.col("nd") > 1).cast("long")).alias("inter_dumps"),
               F.sum(F.col("n") - F.col("nd")).alias("intra_dump"))
          .first())
t_incr = time.time()

mois_complet = sorted(r["mois"] for r in spark.read.parquet(complet).select("mois").distinct().collect())
couverts = sorted(set(mois_anciens) & set(mois_complet))
anciennes_couvertes = i.where(ancien & F.col("mois").isin(couverts)).count() if couverts else 0

a = (i.where(ancien & F.col("mois").isin(couverts))
      .select(F.xxhash64("timestamp", "recording_msid").alias("hp"), F.xxhash64("user_id").alias("hu"))
      .distinct())
c = (spark.read.parquet(complet).where(F.col("mois").isin(couverts))
      .select(F.xxhash64("timestamp", "recording_msid").alias("hp"), F.xxhash64("user_id").alias("hu_c")))
den = a.agg(F.count(F.lit(1)).alias("triplets"), F.countDistinct("hp").alias("paires")).first()
if couverts:
    rec = (a.join(c, "hp", "inner")
            .agg(F.countDistinct("hp").alias("paires"),
                 F.countDistinct(F.when(F.col("hu") == F.col("hu_c"), F.struct("hp", "hu"))).alias("triplets"))
            .first())
    rec_paires, rec_triplets = rec["paires"], rec["triplets"]
else:
    rec_paires = rec_triplets = 0
t_fin = time.time()

def bornes(l):
    return (l[0], l[-1], len(l)) if l else ("-", "-", 0)

print("INCR LIGNES", total, "DUMPS", len(par_dump), "RECENTES", recentes, "ANCIENNES", anciennes, "UID_INVALIDES", uid_invalides)
for d, r, an in par_dump:
    print("  DUMP", d, "recentes", r, "anciennes", an)
print("MOIS_RECENTS min/max/n", *bornes(mois_recents))
print("MOIS_ANCIENS min/max/n", *bornes(mois_anciens))
print("Q1 TRIPLETS_REPETES", multi["triplets"] or 0, "LIGNES_EN_TROP", multi["en_trop"] or 0,
      "DONT_ENTRE_DUMPS", multi["inter_dumps"] or 0, "DANS_UN_MEME_DUMP", multi["intra_dump"] or 0)
print("COMPLET MOIS min/max/n", *bornes(mois_complet))
print("Q2 ANCIENNES_DANS_MOIS_COUVERTS", anciennes_couvertes, "HORS_COUVERTURE", anciennes - anciennes_couvertes,
      "MOIS_COUVERTS", len(couverts))
print("Q2 DISTINCTS triplets", den["triplets"], "paires", den["paires"])
print("Q2 RECOUVERTS triplets", rec_triplets, "paires", rec_paires)
if den["paires"]:
    tp = rec_paires / den["paires"]
    tt = rec_triplets / den["triplets"]
    print("Q2 TAUX triplets", round(tt, 6), "paires", round(tp, 6))
    if tp >= 0.5 and tt < 0.5 * tp:
        print("Q2_TEMOIN_ALERTE jetons probablement differents entre les deux sources : pas de conclusion sur les doublons")
print("PHASES_S demarrage", round(t_session - t_app, 1), "incrementaux", round(t_incr - t_session, 1),
      "recouvrement", round(t_fin - t_incr, 1))
# Controles qui peuvent echouer : jetons au format HMAC tronque ; aucun doublon a l'interieur d'un meme dump
# (3v_24 le garantit, sa violation invaliderait Q1) ; denominateur de Q2 borne par les lignes anciennes couvertes.
ok = uid_invalides == 0 and (multi["intra_dump"] or 0) == 0 and den["triplets"] <= anciennes_couvertes
print("RECOUVREMENT_MESURE" if ok else "RECOUVREMENT_ECHEC")
if not ok:
    sys.exit(1)
