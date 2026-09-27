# 3v_29 : etiquettes MusicBrainz (CC BY-NC-SA 3.0 US) -> variables normalisees, usage academique seulement.
# Politique validee (2.4) : P1 votes <= 0 exclus ; P2 normalisation NFKC, minuscules, / - _ -> espace, & -> and ;
# P3 synonymes versionnes ; P4 vocabulaire >= seuil enregistrements canoniques ; P5 rattachement au MBID canonique ;
# P6 sorties sous curated/trois_v/nc_sa/, tables Glue mb_nc_sa_* avec licence et usage.
# Args : nc_sa_brut recording_id_gid mb correspondance synonymes sortie base seuil ecoutes_totales attendus
#   attendus = "lignes_tag,lignes_recording_tag,lignes_recording,votes_non_positifs"
import sys, time, re, json, unicodedata
t_app = time.time()
from pyspark.sql import SparkSession, functions as F, types as T

(brut, id_gid, mb, corr_chemin, synonymes_chemin, sortie, base, seuil, ecoutes_totales, attendus) = sys.argv[1:11]
seuil, ecoutes_totales = int(seuil), int(ecoutes_totales)
att_tag, att_rt, att_rec, att_neg = [int(x) for x in attendus.split(",")]
spark = SparkSession.builder.appName("3v_etiquettes_musicbrainz").getOrCreate()
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
col = F.col
UUID = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
LICENCE = "CC-BY-NC-SA-3.0-US"
USAGE = "academique-non-diffusable-labels"
SOURCE = "MusicBrainz mbdump-derived 20260923-002121 (recording_tag, tag), CC BY-NC-SA 3.0 US, attribution MetaBrainz Foundation"

def lire_json(chemin):
    if chemin.startswith("s3://"):
        import boto3
        b, k = chemin[5:].split("/", 1)
        return json.loads(boto3.client("s3").get_object(Bucket=b, Key=k)["Body"].read().decode("utf-8"))
    with open(chemin, encoding="utf-8") as f:
        return json.load(f)

ECHAPPEMENTS = {"\\\\": "\\", "\\t": "\t", "\\n": "\n", "\\r": "\r", "\\b": "\b", "\\f": "\f", "\\v": "\v"}
def decoder_copy(s):
    # format texte COPY PostgreSQL : echappements par barre oblique inverse
    return re.sub(r"\\[\\tnrbfv]", lambda m: ECHAPPEMENTS[m.group(0)], s)

def normaliser(nom, synonymes):
    s = unicodedata.normalize("NFKC", decoder_copy(nom)).lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[/\-_]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = synonymes.get(s, s)
    return s or None

def lire_tsv(chemin, colonnes, types):
    brut_df = spark.read.text(chemin)
    parts = F.split(col("value"), "\t")
    df = brut_df.select(parts.alias("p"))
    mauvais = df.where(F.size("p") != len(colonnes)).count()
    df = df.select(*[col("p")[i].cast(t).alias(c) for i, (c, t) in enumerate(zip(colonnes, types))])
    return df, mauvais

if __name__ == "__main__":
    synonymes = lire_json(synonymes_chemin)["synonymes"]
    assert all(normaliser(k, {}) == k and normaliser(v, {}) == v for k, v in synonymes.items()), "synonymes non normalises"
    ok = True

    # 1. Lecture et controles de structure
    t0 = time.time()
    tag, m_tag = lire_tsv(brut + "/tag/", ["tag_id", "nom", "ref_count"], ["long", "string", "long"])
    rt, m_rt = lire_tsv(brut + "/recording_tag/", ["rec_id", "tag_id", "votes", "maj"], ["long", "long", "long", "string"])
    rec, m_rec = lire_tsv(id_gid, ["rec_id", "gid"], ["long", "string"])
    rt = rt.cache(); rec = rec.repartition(64).cache()
    n_tag, n_rt, n_rec = tag.count(), rt.count(), rec.count()
    n_neg = rt.where(col("votes") <= 0).count()
    nulls = rt.where(col("rec_id").isNull() | col("tag_id").isNull() | col("votes").isNull()).count()
    rec_ko = rec.where(col("rec_id").isNull() | ~col("gid").rlike(UUID)).count()
    t_ok = (n_tag, n_rt, n_rec, n_neg) == (att_tag, att_rt, att_rec, att_neg) and m_tag == m_rt == m_rec == 0 and nulls == 0 and rec_ko == 0
    ok &= t_ok
    print("LECTURE tag", n_tag, "recording_tag", n_rt, "recording", n_rec, "votes_non_positifs", n_neg,
          "ATTENDUS", attendus, "colonnes_ko", m_tag, m_rt, m_rec, "nulls", nulls, "recording_ko", rec_ko,
          "OK" if t_ok else "ECHEC", "duree_s", round(time.time() - t0, 1), flush=True)
    if not ok:
        print("ETIQUETTES_ECHEC lecture, aucune ecriture", "duree_s", round(time.time() - t_app, 1)); sys.exit(1)

    # 2. Normalisation (table tag, petite : calcul sur le pilote, reproductible et testable)
    lignes = tag.select("tag_id", "nom").collect()
    avec_echappement = sum(1 for r in lignes if "\\" in r["nom"])
    norm = [(r["tag_id"], r["nom"], normaliser(r["nom"], synonymes)) for r in lignes]
    par_syn = sum(1 for _, n, e in norm if normaliser(n, {}) in synonymes)
    vides = sum(1 for _, _, e in norm if e is None)
    tnorm = spark.createDataFrame(norm, "tag_id long, nom string, etiquette string").where(col("etiquette").isNotNull())
    print("NORMALISATION noms", len(norm), "distincts_normalises", len({e for _, _, e in norm if e}),
          "avec_echappement", avec_echappement, "via_synonyme", par_syn, "vides", vides, flush=True)

    # 3. P1 + rattachement enregistrement -> gid -> MBID canonique (P5)
    canon = spark.read.parquet(mb + "/mb_canonical_recording").select(col("recording_mbid").alias("gid"), F.lit("direct").alias("voie"))
    redir = spark.read.parquet(mb + "/mb_recording_redirect").select(col("recording_mbid").alias("gid"),
                                                                     col("canonical_recording_mbid").alias("canon_r"))
    pos = rt.where(col("votes") > 0)
    sans_gid = pos.join(rec, "rec_id", "left_anti").count()
    g = (pos.join(rec, "rec_id").join(tnorm, "tag_id")
         .join(canon, "gid", "left").join(redir, "gid", "left")
         .withColumn("voie", F.when(col("voie").isNotNull(), F.lit("direct"))
                             .when(col("canon_r").isNotNull(), F.lit("redirection")).otherwise(F.lit("hors_canonique")))
         .withColumn("recording_mbid", F.when(col("voie") == "direct", col("gid")).otherwise(col("canon_r"))))
    g = g.cache()
    voies = {r["voie"]: (r["lignes"], r["enr"]) for r in
             g.groupBy("voie").agg(F.count(F.lit(1)).alias("lignes"), F.countDistinct("gid").alias("enr")).collect()}
    print("RATTACHEMENT sans_gid", sans_gid, "voies (lignes, enregistrements)", voies, flush=True)
    ok &= sans_gid == 0
    if not ok:
        print("ETIQUETTES_ECHEC rattachement, aucune ecriture", "duree_s", round(time.time() - t_app, 1)); sys.exit(1)

    # 4. Agregation par (MBID canonique, etiquette) puis vocabulaire (P4)
    ag = (g.where(col("recording_mbid").isNotNull())
          .groupBy("recording_mbid", "etiquette").agg(F.sum("votes").alias("votes")))
    ag = ag.cache()
    stats = (ag.groupBy("etiquette").agg(F.count(F.lit(1)).alias("nb_enregistrements"), F.sum("votes").alias("nb_votes")))
    noms = (tnorm.join(pos.select("tag_id").distinct(), "tag_id")
            .groupBy("etiquette").agg(F.sort_array(F.collect_set("nom")).alias("noms_bruts")))
    tout = stats.join(noms, "etiquette", "left").withColumn("nb_noms_bruts", F.size("noms_bruts"))
    for s in (10, 50, 100, 1000):
        print("VOCABULAIRE seuil", s, "etiquettes", tout.where(col("nb_enregistrements") >= s).count(), flush=True)
    vocab = tout.where(col("nb_enregistrements") >= seuil).select("etiquette", "nb_enregistrements", "nb_votes",
                                                                  "nb_noms_bruts", "noms_bruts")
    fusions = vocab.where(col("nb_noms_bruts") > 1).orderBy(col("nb_enregistrements").desc())
    print("FUSIONS dans le vocabulaire", fusions.count(), flush=True)
    for r in fusions.limit(40).collect():
        print("FUSION", r["etiquette"], "|", r["nb_enregistrements"], "|", " ; ".join(r["noms_bruts"][:8]), flush=True)

    # 5. Ecriture
    s_vocab, s_enr = sortie + "/mb_nc_sa_vocabulaire", sortie + "/mb_nc_sa_etiquettes_enregistrement"
    vocab.repartition(1).write.mode("overwrite").parquet(s_vocab)
    (ag.join(vocab.select("etiquette"), "etiquette", "left_semi")
       .select("recording_mbid", "etiquette", "votes").repartition(8).write.mode("overwrite").parquet(s_enr))
    rv, re_ = spark.read.parquet(s_vocab), spark.read.parquet(s_enr)
    v = rv.agg(F.count(F.lit(1)).alias("n"), F.countDistinct("etiquette").alias("d"),
               F.min("nb_enregistrements").alias("mn")).first()
    e = re_.agg(F.count(F.lit(1)).alias("n"), F.countDistinct("recording_mbid").alias("enr"),
                F.sum((~col("recording_mbid").rlike(UUID)).cast("long")).alias("non_uuid"),
                F.min("votes").alias("vmin")).first()
    doublons = re_.groupBy("recording_mbid", "etiquette").count().where(col("count") > 1).count()
    hors_vocab = re_.join(rv, "etiquette", "left_anti").count()
    t_ok = (v["n"] == v["d"] and v["n"] > 0 and (v["mn"] or 0) >= seuil and e["non_uuid"] == 0
            and (e["vmin"] or 0) > 0 and doublons == 0 and hors_vocab == 0)
    ok &= t_ok
    print("SORTIE vocabulaire", v["n"], "lignes_enr_etiquette", e["n"], "enregistrements", e["enr"],
          "non_uuid", e["non_uuid"], "votes_min", e["vmin"], "doublons", doublons, "hors_vocab", hors_vocab,
          "OK" if t_ok else "ECHEC", flush=True)

    # 6. Couverture en ecoutes via mb_correspondance_msid
    corr = spark.read.parquet(corr_chemin)
    etiquetes = re_.select("recording_mbid").distinct()
    c = (corr.join(etiquetes, "recording_mbid", "left_semi").agg(F.sum("ecoutes_choix").alias("choix"),
                                                                  F.sum("ecoutes_msid").alias("msid"),
                                                                  F.count(F.lit(1)).alias("n")).first())
    resolues = corr.agg(F.sum("ecoutes_msid").alias("s")).first()["s"]
    pct = lambda a, b: round(100.0 * (a or 0) / b, 2) if b else 0.0
    print("COUVERTURE msid_etiquetes", c["n"], "ecoutes", c["msid"], "sur_total", ecoutes_totales,
          "(%s %%)" % pct(c["msid"], ecoutes_totales), "sur_resolues", resolues, "(%s %%)" % pct(c["msid"], resolues), flush=True)

    # 7. Catalogue Glue (P6)
    if base != "-" and ok:
        import boto3
        from botocore.exceptions import ClientError
        glue = boto3.client("glue", region_name="eu-north-1")
        TG = {"StringType()": "string", "LongType()": "bigint", "IntegerType()": "int", "ArrayType(StringType(), True)": "array<string>",
              "ArrayType(StringType(), False)": "array<string>"}
        def declarer(nom, emplacement, schema, description):
            d = {"Name": nom, "Description": description, "TableType": "EXTERNAL_TABLE",
                 "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE", "projet": "SoundLab", "tache": "3v_2.4",
                                "licence": LICENCE, "usage": USAGE, "source": SOURCE},
                 "StorageDescriptor": {"Columns": [{"Name": f.name, "Type": TG.get(str(f.dataType), "string")} for f in schema.fields],
                                       "Location": emplacement,
                                       "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                                       "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                                       "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                                     "Parameters": {"serialization.format": "1"}}, "Compressed": True}}
            try:
                glue.create_table(DatabaseName=base, TableInput=d); return "creee"
            except ClientError as err:
                if err.response["Error"]["Code"] != "AlreadyExistsException":
                    raise
                glue.update_table(DatabaseName=base, TableInput=d); return "mise_a_jour"
        print("GLUE", "mb_nc_sa_vocabulaire", declarer("mb_nc_sa_vocabulaire", s_vocab, rv.schema,
              "Vocabulaire d'etiquettes normalisees (NC-SA, non diffusable aux labels)"),
              "mb_nc_sa_etiquettes_enregistrement", declarer("mb_nc_sa_etiquettes_enregistrement", s_enr, re_.schema,
              "Etiquettes par enregistrement canonique, poids = votes (NC-SA, non diffusable aux labels)"), flush=True)
    else:
        print("GLUE saute", flush=True)
    print("ETIQUETTES_OK" if ok else "ETIQUETTES_ECHEC", "duree_s", round(time.time() - t_app, 1))
    if not ok:
        sys.exit(1)
