import sys, time, collections
t_app = time.time()
from functools import reduce
from pyspark.sql import SparkSession, functions as F

brut, debut, fin, sortie, attendu, temoin_mois, temoin_attendu = sys.argv[1:8]
attendu, temoin_attendu = int(attendu), int(temoin_attendu)
CLES = ["ip_addr", "conn_country", "platform", "offline", "offline_timestamp", "shuffle",
        "spotify_episode_uri", "episode_name", "episode_show_name", "audiobook_title",
        "audiobook_uri", "audiobook_chapter_uri", "audiobook_chapter_title"]
if len(sys.argv) > 8:
    CLES = sys.argv[8].split(",")
CLE_TEMOIN = CLES[0]
spark = SparkSession.builder.appName("3v_cles_sensibles_brut").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

txt = (spark.read.option("basePath", brut).text(brut + "/date=*/part-2663.json.zst")
       .where(F.col("date").between(F.lit(debut).cast("date"), F.lit(fin).cast("date"))))
v = F.col("value")
pre = reduce(lambda a, b: a | b, [F.instr(v, k) > 0 for k in CLES])
presence = {k: v.rlike('"' + k + '"\\s*:') for k in CLES}
une = reduce(lambda a, b: a | b, presence.values())
aggs = [F.count(F.lit(1)).alias("lignes"),
        F.sum(F.when(pre & une, 1).otherwise(0)).alias("touchees")]
aggs += [F.sum(F.when(pre & presence[k], 1).otherwise(0)).alias(k) for k in CLES]
jours = txt.groupBy(F.date_format("date", "yyyy-MM-dd").alias("jour")).agg(*aggs).collect()
t_lecture = time.time()

lues = sum(r["lignes"] for r in jours)
touches = sorted((r for r in jours if r["touchees"] > 0), key=lambda r: r["jour"])
par_cle = {k: sum(r[k] for r in jours) for k in CLES}
par_mois = collections.OrderedDict()
for r in touches:
    m = par_mois.setdefault(r["jour"][:7], [0, 0])
    m[0] += 1
    m[1] += r["touchees"]
temoin = sum(r[CLE_TEMOIN] for r in jours if r["jour"].startswith(temoin_mois))
if touches:
    (spark.createDataFrame([(r["jour"], r["lignes"], r["touchees"], r[CLE_TEMOIN]) for r in touches],
                           "jour string, lignes long, touchees long, cle_temoin long")
          .coalesce(1).write.mode("overwrite").option("header", True).csv(sortie))
t_fin = time.time()

print("LUES", lues, "ATTENDU", attendu, "JOURS_LUS", len(jours))
print("TOUCHEES", sum(r["touchees"] for r in touches), "JOURS_TOUCHES", len(touches), "MOIS_TOUCHES", len(par_mois))
if touches:
    print("PREMIER_JOUR", touches[0]["jour"], "DERNIER_JOUR", touches[-1]["jour"])
for k in CLES:
    print("  CLE", k, par_cle[k])
for m, (j, n) in par_mois.items():
    print("  MOIS", m, "jours", j, "lignes", n)
print("TEMOIN", temoin_mois, CLE_TEMOIN, temoin, "ATTENDU", temoin_attendu)
print("PHASES_S lecture", round(t_lecture - t_app, 1), "ecriture", round(t_fin - t_lecture, 1))
ok = lues == attendu and temoin == temoin_attendu
print("RECHERCHE_OK" if ok else "RECHERCHE_ECHEC")
if not ok:
    sys.exit(1)
