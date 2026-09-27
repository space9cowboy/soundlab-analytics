import sys, time
t_app = time.time()
from pyspark.sql import SparkSession, functions as F

# AN1 : mesure de la jointure catalogue Kaggle <-> ecoutes ListenBrainz, par trois voies.
# Lecture : table aplatie ListenBrainz, music_info, songs_features_labeled, referentiel MusicBrainz.
# Ecriture : uniquement sous le prefixe d'analyse passe en argument (identifiants de titres et comptages,
# aucune donnee d'auditeur).
aplati, cur, sortie, attendu, kaggle_attendu = sys.argv[1:6]
attendu, kaggle_attendu = int(attendu), int(kaggle_attendu)
spark = SparkSession.builder.appName("3v_an1_jointure_kaggle_lb").getOrCreate()
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
col, e = F.col, F.col("ecoutes")
TRANCHES = [("2002-01", "2008-12")] + [("%d-01" % a, "%d-12" % a) for a in range(2009, 2017)]
pct = lambda a, b: round(100.0 * a / b, 2) if b else 0.0

def cle_ascii(texte):
    return F.when(texte.rlike("^[\\x00-\\x7F]*$"),
                  F.nullif(F.regexp_replace(F.lower(texte), "[^a-z0-9_]", ""), F.lit("")))

def sp_format(x):
    return (F.when(x.isNull(), "absent")
             .when(x.rlike("^https?://open\\.spotify\\.com/track/[0-9A-Za-z]{22}"), "url_track")
             .when(x.rlike("^spotify:track:[0-9A-Za-z]{22}$"), "uri_track")
             .when(x.rlike("^[0-9A-Za-z]{22}$"), "nu_22")
             .otherwise("autre"))

def sp_extrait(x):
    return (F.when(x.rlike("^[0-9A-Za-z]{22}$"), x)
             .when(x.rlike("track[/:][0-9A-Za-z]{22}"), F.regexp_extract(x, "track[/:]([0-9A-Za-z]{22})", 1)))

# 1. Catalogue Kaggle
mi = spark.read.parquet(cur + "/music_info")
n_mi = mi.count()
w_mi = mi.groupBy("track_id").agg(F.max("date_ingestion").alias("date_ingestion"))
k = mi.join(w_mi, ["track_id", "date_ingestion"]).select("track_id", "artist", "name", "spotify_id").dropDuplicates(["track_id"])
n_k = k.count()
k = k.withColumn("cle", cle_ascii(F.concat_ws("", col("artist"), col("name")))).withColumn("sp", sp_extrait(col("spotify_id")))
kf = {r["f"]: r["n"] for r in k.groupBy(sp_format(col("spotify_id")).alias("f")).agg(F.count(F.lit(1)).alias("n")).collect()}
kc = k.agg(F.sum(col("cle").isNull().cast("long")).alias("non_ascii")).first()
k_cle = k.where(col("cle").isNotNull()).groupBy("cle").agg(F.count(F.lit(1)).alias("n"), F.min("track_id").alias("t_cle"))
amb_cle = k_cle.where(col("n") > 1).agg(F.count(F.lit(1)).alias("c"), F.sum("n").alias("t")).first()
k_cle = k_cle.where(col("n") == 1).drop("n")
k_sp = k.where(col("sp").isNotNull()).groupBy("sp").agg(F.count(F.lit(1)).alias("n"), F.min("track_id").alias("t_sp"))
amb_sp = k_sp.where(col("n") > 1).agg(F.count(F.lit(1)).alias("c"), F.sum("n").alias("t")).first()
k_sp = k_sp.where(col("n") == 1).drop("n")
print("KAGGLE lignes", n_mi, "titres", n_k, "ATTENDU", kaggle_attendu, "cle_non_ascii", kc["non_ascii"],
      "cles_ambigues", amb_cle["c"] or 0, "titres_ambigus", amb_cle["t"] or 0,
      "spotify_formats", dict(sorted(kf.items())), "spotify_ambigus", amb_sp["c"] or 0, flush=True)

# 2. Referentiel MusicBrainz : Kaggle -> enregistrement canonique par la cle (regle de 3v_27, reverifiee)
canon = spark.read.parquet(cur + "/trois_v/musicbrainz/mb_canonical_recording")
nom = F.concat_ws("", col("artist_credit_name"), col("recording_name"))
h = (canon.where(nom.rlike("^[\\x00-\\x7F]*$"))
     .agg(F.count(F.lit(1)).alias("n"), F.sum((cle_ascii(nom) == col("combined_lookup")).cast("long")).alias("ok")).first())
accord_cle = h["ok"] / h["n"] if h["n"] else 0.0
print("HYPOTHESE_CLE lignes_ascii", h["n"], "accord", h["ok"], "taux", round(accord_cle, 5), flush=True)
kmap = (canon.groupBy(col("combined_lookup").alias("cle")).agg(F.count(F.lit(1)).alias("n"),
                                                               F.min("recording_mbid").alias("mbid"))
        .where(col("n") == 1).drop("n"))
k_mb = k_cle.join(kmap, "cle").groupBy("mbid").agg(F.count(F.lit(1)).alias("n"), F.min("t_cle").alias("t_mb"))
k_mb = k_mb.where(col("n") == 1).drop("n")
n_k_mb = k_mb.count()
corr = spark.read.parquet(cur + "/trois_v/musicbrainz/mb_correspondance_msid").select("recording_msid", col("recording_mbid").alias("mbid"))
print("KAGGLE_VERS_MB titres_relies", n_k_mb, "(%s %%)" % pct(n_k_mb, n_k), flush=True)

# 3. Ecoutes agregees par combinaison (tranches : brassage borne, comme 3v_27)
t0 = time.time()
src = spark.read.parquet(aplati)
tmp = sortie + "/_tmp_combos"
for i, (a, b) in enumerate(TRANCHES):
    (src.where(col("mois").between(a, b))
        .select("recording_msid",
                cle_ascii(F.concat_ws("", col("artist_name"), col("track_name"))).alias("cle"),
                F.coalesce(sp_extrait(col("spotify_id")), sp_extrait(col("spotify_track_uri"))).alias("sp"),
                sp_format(col("spotify_id")).alias("fmt"))
        .groupBy("recording_msid", "cle", "sp", "fmt").agg(F.count(F.lit(1)).alias("ecoutes"))
        .write.mode("overwrite" if i == 0 else "append").parquet(tmp))
combos = spark.read.parquet(tmp)
tot = combos.agg(F.sum(e).alias("e"), F.count(F.lit(1)).alias("c")).first()
lf = {r["fmt"]: r["s"] for r in combos.groupBy("fmt").agg(F.sum(e).alias("s")).collect()}
print("COMBOS lignes", tot["c"], "ECOUTES", tot["e"], "ATTENDU", attendu, "duree_s", round(time.time() - t0, 1), flush=True)
print("LB_SPOTIFY_FORMATS", {x: (v, pct(v, tot["e"])) for x, v in sorted(lf.items())}, flush=True)

# 4. Trois voies
r = (combos.join(F.broadcast(k_cle), "cle", "left")
           .join(F.broadcast(k_sp), "sp", "left")
           .join(corr, "recording_msid", "left")
           .join(F.broadcast(k_mb), "mbid", "left")
           .withColumn("track_id", F.coalesce("t_sp", "t_mb", "t_cle"))
           .withColumn("voie", F.when(col("t_sp").isNotNull(), "spotify").when(col("t_mb").isNotNull(), "musicbrainz")
                                .when(col("t_cle").isNotNull(), "cle_artiste_titre")))
res = sortie + "/an1_combos_resolus"
r.write.mode("overwrite").parquet(res)
r = spark.read.parquet(res)
m = r.agg(F.sum(e).alias("total"),
          *[F.sum(F.when(col(c).isNotNull(), e).otherwise(0)).alias(c) for c in ("cle", "sp", "mbid", "t_cle", "t_sp", "t_mb", "track_id")]).first()
print("LB_PRESENCE cle", m["cle"], "(%s %%)" % pct(m["cle"], m["total"]), "spotify", m["sp"], "(%s %%)" % pct(m["sp"], m["total"]),
      "mbid_via_correspondance", m["mbid"], "(%s %%)" % pct(m["mbid"], m["total"]), flush=True)
for nomv, c in (("cle_artiste_titre", "t_cle"), ("spotify", "t_sp"), ("musicbrainz", "t_mb")):
    d = r.where(col(c).isNotNull()).select(c).distinct().count()
    print("VOIE", nomv, "ecoutes_couvertes", m[c], "(%s %%)" % pct(m[c], m["total"]),
          "titres_kaggle", d, "(%s %%)" % pct(d, n_k), flush=True)
d_all = r.where(col("track_id").isNotNull()).select("track_id").distinct().count()
print("UNION ecoutes_couvertes", m["track_id"], "(%s %%)" % pct(m["track_id"], m["total"]),
      "titres_kaggle", d_all, "(%s %%)" % pct(d_all, n_k), flush=True)

def accord(a, b, nomp):
    x = r.where(col(a).isNotNull() & col(b).isNotNull()).agg(
        F.sum(e).alias("ecoutes"), F.sum(F.when(col(a) == col(b), e).otherwise(0)).alias("acc")).first()
    print("ACCORD", nomp, "ecoutes", x["ecoutes"] or 0, "identiques", x["acc"] or 0,
          "taux_%", pct(x["acc"] or 0, x["ecoutes"] or 0), flush=True)

accord("t_sp", "t_cle", "spotify_vs_cle")
accord("t_sp", "t_mb", "spotify_vs_musicbrainz")
accord("t_mb", "t_cle", "musicbrainz_vs_cle")

# 5. Couverture selon la cible Kaggle (biais de selection)
lab = spark.read.parquet(cur + "/songs_features_labeled").select("track_id", "is_hit").dropDuplicates(["track_id"])
trouves = r.where(col("track_id").isNotNull()).groupBy("track_id").agg(F.sum(e).alias("ecoutes_lb"))
cb = (lab.join(trouves, "track_id", "left").groupBy("is_hit")
      .agg(F.count(F.lit(1)).alias("titres"), F.count("ecoutes_lb").alias("trouves"),
           F.expr("percentile_approx(ecoutes_lb, 0.5)").alias("mediane_ecoutes_lb")).orderBy("is_hit").collect())
for x in cb:
    print("COUVERTURE_CIBLE is_hit", x["is_hit"], "titres", x["titres"], "trouves", x["trouves"],
          "(%s %%)" % pct(x["trouves"], x["titres"]), "mediane_ecoutes_lb", x["mediane_ecoutes_lb"], flush=True)
hors_lab = trouves.join(lab, "track_id", "left_anti").count()
print("TROUVES_HORS_ETIQUETES", hors_lab, flush=True)

ok = (tot["e"] == attendu and m["total"] == attendu and n_k == kaggle_attendu and accord_cle >= 0.99
      and m["track_id"] <= m["total"] and max(m["t_cle"], m["t_sp"], m["t_mb"]) <= m["track_id"]
      and m["track_id"] <= m["t_cle"] + m["t_sp"] + m["t_mb"])
print("AN1_OK" if ok else "AN1_ECHEC", "duree_s", round(time.time() - t_app, 1), flush=True)
if not ok:
    sys.exit(1)
