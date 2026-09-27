import sys, time, re, collections, math, json
t_app = time.time()
from pyspark.sql import SparkSession, functions as F, types as T

# v4 (tache 3.1, D1-D3) : option --incremental <n dump> ; lit la zone de transit
# <brut>/dump=<n>/part-<n>.json.zst, tire la date de l'horodatage, dedoublonne le triplet
# (user_id, timestamp, recording_msid) dans le lot et ecrit en partitions mois=/dump=
# (ecrasement dynamique : recharger un dump remplace exactement ses partitions).
# v5 (tache 3.2, option C) : partitions mois=/jour=/dump= ; jour = date d'ecoute si le retard par rapport
# au jour de reception (--reception, debut de fenetre du manifeste) est <= 30 jours (negatifs compris),
# sinon _ancien. Une ecoute d'hier recue aujourd'hui atterrit dans jour=<hier>.
# v6 (tache 3.3, decision de conception apres l'echec du dump 2680) : une ligne dont un champ garde a change de type
# (TYPE_MODIFIE) part au rebut avec le motif type_modifie:<champs> au lieu d'arreter tout le dump, tant que ces
# lignes restent <= TM_TAUX_MAX des lignes lues ET touchent au plus TM_CHAMPS_MAX champ distinct. Au-dela :
# echec du contrat, aucune ecriture, comme en v5. Les lignes suspectes ou non conformes sont diagnostiquees
# ligne a ligne (la valeur n'est transmise a Python que pour elles).
# v7 (tache 3.3, echec du dump 2683 lors d'un chargement nocturne) : un dump vide (0 ligne lue, 0 attendue, contrat respecte) est un
# cas normal : aucune ecriture, SORTIE LIGNES 0, DUMP_VIDE, APLATI_OK. Tout autre lot sans ligne a ecrire
# (lignes lues mais toutes au rebut ou incognito, ou attendu different de 0) echoue : APLATI_VIDE_NON_ATTENDU.
ARGS = sys.argv[1:]
DUMP = None
if "--incremental" in ARGS:
    i = ARGS.index("--incremental")
    DUMP = ARGS[i + 1]
    assert DUMP.isdigit(), "numero de dump attendu"
    del ARGS[i:i + 2]
RECEPTION = None
if "--reception" in ARGS:
    i = ARGS.index("--reception")
    RECEPTION = ARGS[i + 1]
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", RECEPTION), "date de reception AAAA-MM-JJ attendue"
    del ARGS[i:i + 2]
INCR = DUMP is not None
assert not INCR or RECEPTION, "--reception obligatoire avec --incremental (v5)"
RETARD_MAX_JOUR = 30
TM_TAUX_MAX = 1e-4
TM_CHAMPS_MAX = 1
sys.argv = [sys.argv[0]] + ARGS
brut, debut, fin, sortie_aplati, sortie_rebut, attendu = sys.argv[1:7]
attendu = int(attendu)
CONTRAT = sys.argv[7] if len(sys.argv) > 7 else "s3://soundlab-scripts-558852/trois_v/contrats/3v_contrat_listenbrainz_v1.json"
CONTROLE_SEUL = len(sys.argv) > 8 and sys.argv[8] == "controle"
spark = SparkSession.builder.appName("3v_aplatissement").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
CIBLE_FICHIER = 256 * 2 ** 20
OCTETS_PAR_LIGNE = 134.7
t_session = time.time()

contrat = json.loads(spark.sparkContext.wholeTextFiles(CONTRAT).collect()[0][1])
NIV = contrat["niveaux"]
print("CONTRAT", CONTRAT, "version", contrat["version"], flush=True)
S = T.StringType()
L = T.ArrayType(T.StringType())

def type_spark(d):
    t = d["type"]
    if t in ("texte", "texte_ou_entier"):
        return S
    if t == "entier":
        return T.LongType()
    if t == "booleen":
        return T.BooleanType()
    if t == "liste_texte":
        return L
    if t == "objet":
        return struct_niveau(d["niveau"])
    if t == "liste_objet":
        return T.ArrayType(struct_niveau(d["niveau"]))
    raise ValueError("type inconnu " + t)

def struct_niveau(n):
    return T.StructType([T.StructField(k, type_spark(d)) for k, d in NIV[n]["gardes"].items()])

SCHEMA = T.StructType(struct_niveau("racine").fields + [T.StructField("_rebut", S)])
INTERDITES = set(contrat["interdites_toujours"])
PREFIXE = {"racine": "racine.", "tm": "tm.", "ai": "ai.", "mm": "mm."}
NIV_DE = {v: k for k, v in PREFIXE.items()}
AI_ECARTEES = sorted((set().union(*[set(v["ecartes"]) | set(v["purges"]) for v in NIV.values()]) | INTERDITES) - {"date"})
NIVEAUX = [(NIV[n]["chemin"], PREFIXE[n], list(NIV[n]["gardes"])) for n in ("racine", "tm", "ai", "mm")]
CHEMIN_COL = {"racine": "e.", "tm": "e.track_metadata.", "ai": "e.track_metadata.additional_info.",
              "mm": "e.track_metadata.mbid_mapping."}
OBLIGATOIRES = [(n, k) for n in ("racine", "tm") for k, d in NIV[n]["gardes"].items() if d.get("obligatoire")]
FACULTATIFS = [(n, k) for n in ("tm", "ai", "mm") for k, d in NIV[n]["gardes"].items() if not d.get("obligatoire")]
T_TEXTE = sorted({k for v in NIV.values() for k, d in v["gardes"].items() if d["type"] == "texte"})
T_MIXTE = sorted({k for v in NIV.values() for k, d in v["gardes"].items() if d["type"] == "texte_ou_entier"})
SUSPECT = ('"(' + "|".join(map(re.escape, T_TEXTE)) + r')"\s*:\s*[-0-9tf\[{]|"('
           + "|".join(map(re.escape, T_MIXTE)) + r')"\s*:\s*[tf\[{]')

def nom_type(v):
    if v is None:
        return "nul"
    if isinstance(v, bool):
        return "booleen"
    if isinstance(v, int):
        return "entier"
    if isinstance(v, float):
        return "reel"
    if isinstance(v, str):
        return "texte"
    if isinstance(v, list):
        return "liste"
    return "objet"

def type_ok(t, v):
    if v is None:
        return True
    n = nom_type(v)
    if t == "texte_ou_entier":
        return n in ("texte", "entier")
    if t == "liste_texte":
        return n == "liste" and all(x is None or isinstance(x, str) for x in v)
    if t == "liste_objet":
        return n == "liste" and all(isinstance(x, dict) for x in v)
    return n == t

def diag(niv, prefixe, o, out):
    for k, d in NIV[niv]["gardes"].items():
        if k not in o:
            continue
        v = o[k]
        if not type_ok(d["type"], v):
            out.append("TYPE_MODIFIE %s%s attendu %s vu %s" % (prefixe, k, d["type"], nom_type(v)))
        elif d["type"] == "objet" and v is not None:
            diag(d["niveau"], PREFIXE.get(d["niveau"], prefixe + k + "."), v, out)
        elif d["type"] == "liste_objet" and v:
            for x in v:
                diag(d["niveau"], prefixe + k + "[].", x, out)

@F.udf(returnType=L)
def diagnostiquer(valeur):
    try:
        o = json.loads(valeur)
    except Exception:
        return ["JSON_INVALIDE"]
    if not isinstance(o, dict):
        return ["JSON_INVALIDE"]
    out = []
    diag("racine", "racine.", o, out)
    return sorted(set(out)) or ["AUCUN_ECART_DE_TYPE"]

def inconnues(chemin, prefixe, connues):
    k = F.expr(f"json_object_keys(get_json_object(value, '{chemin}'))") if chemin else F.expr("json_object_keys(value)")
    diff = F.coalesce(F.array_except(k, F.array(*[F.lit(x) for x in connues])), F.array().cast(L))
    return F.transform(diff, lambda x: F.concat(F.lit(prefixe), x))

@F.udf(returnType=L)
def champs_type_modifie(valeur):
    if valeur is None:
        return None
    try:
        o = json.loads(valeur)
    except Exception:
        return None
    if not isinstance(o, dict):
        return None
    out = []
    diag("racine", "racine.", o, out)
    ch = sorted({m.split(" ")[1] for m in out if m.startswith("TYPE_MODIFIE")})
    return ch or None

cles = F.concat(*[inconnues(c, p, k) for c, p, k in NIVEAUX])
col = F.col
A = "e.track_metadata.additional_info."
M = "e.track_metadata.mbid_mapping."
TMP = "e.track_metadata."
TN = ("coalesce(e.track_metadata.additional_info.tracknumber, "
      "e.track_metadata.additional_info.trackNumer, e.track_metadata.additional_info.track_number)")
tn_int = F.expr(f"try_cast({TN} as int)")
dm_int = F.col("e.track_metadata.additional_info.duration_ms")

if INCR:
    txt = (spark.read.text(f"{brut}/dump={DUMP}/part-{DUMP}.json.zst")
           .withColumn("date", F.to_date(F.from_unixtime(F.get_json_object("value", "$.timestamp").cast("long")))))
else:
    txt = (spark.read.option("basePath", brut).text(brut + "/date=*/part-2663.json.zst")
           .where(col("date").between(F.lit(debut).cast("date"), F.lit(fin).cast("date"))))
p = txt.select("date", "value", F.from_json("value", SCHEMA, {
    "mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_rebut"}).alias("e"))
json_valide = F.expr("json_object_keys(value)").isNotNull()
non_conforme = col("e").isNull() | col("e._rebut").isNotNull()
p = (p.withColumn("suspect", col("value").rlike(SUSPECT))
      .withColumn("ecarts_type", champs_type_modifie(F.when(col("suspect") | non_conforme, col("value")))))
motif = (F.when(non_conforme & ~json_valide, "json_invalide")
         .when(col("ecarts_type").isNotNull(), F.concat(F.lit("type_modifie:"), F.array_join(col("ecarts_type"), ",")))
         .when(non_conforme, "type_non_conforme"))
for n, k in OBLIGATOIRES:
    motif = motif.when(col(CHEMIN_COL[n] + k).isNull(), "champ_disparu:" + PREFIXE[n] + k)
incognito = col(A + "incognito_mode") == F.lit(True)
p2 = (p.withColumn("motif", motif)
       .withColumn("categorie", F.when(col("motif").isNotNull(), "rebut")
                                 .when(incognito, "incognito").otherwise("aplati"))
       .withColumn("mois", F.date_format("date", "yyyy-MM")))

premier = col("pos").isNull() | (col("pos") == 0)
met = (p2.select("categorie", "motif", "mois",
                 (F.expr(TN).isNotNull() & tn_int.isNull()).cast("long").alias("tn_echec"),
                 col("suspect").cast("long").alias("suspect"),
                 *[col(CHEMIN_COL[n] + k).isNotNull().cast("long").alias("pres_" + PREFIXE[n] + k) for n, k in FACULTATIFS],
                 F.posexplode_outer(cles))
         .groupBy("categorie", "motif", "mois", "col")
         .agg(F.sum(F.when(premier, 1).otherwise(0)).alias("lignes"),
              F.count(F.lit(1)).alias("occurrences"),
              F.sum(F.when(premier, col("tn_echec")).otherwise(0)).alias("tn_echec"),
              F.sum(F.when(premier, col("suspect")).otherwise(0)).alias("suspect"),
              *[F.sum(F.when(premier, col("`pres_" + PREFIXE[n] + k + "`")).otherwise(0)).alias("pres_" + PREFIXE[n] + k)
                for n, k in FACULTATIFS])
         .collect())
t_mesure = time.time()

lues = sum(r["lignes"] for r in met)
par_cat = collections.Counter()
par_motif = collections.Counter()
aplati_mois = collections.Counter()
rebut_mois = set()
cles_inc = collections.Counter()
tn_echec = suspects = 0
presence = collections.Counter()
for r in met:
    par_cat[r["categorie"]] += r["lignes"]
    if r["motif"]:
        par_motif[r["motif"]] += r["lignes"]
    if r["categorie"] == "rebut":
        rebut_mois.add(r["mois"])
    if r["categorie"] == "aplati":
        aplati_mois[r["mois"]] += r["lignes"]
    if r["col"] is not None:
        cles_inc[r["col"]] += r["occurrences"]
    tn_echec += r["tn_echec"]
    suspects += r["suspect"]
    for n, k in FACULTATIFS:
        presence[PREFIXE[n] + k] += r["pres_" + PREFIXE[n] + k]
n_aplati, n_rebut, n_incognito = par_cat["aplati"], par_cat["rebut"], par_cat["incognito"]
print("LUES", lues, "ATTENDU", attendu, flush=True)
print("CATEGORIES aplati", n_aplati, "rebut", n_rebut, "incognito", n_incognito, flush=True)
print("MOTIFS_REBUT", dict(par_motif), flush=True)
print("CLES_INCONNUES", len(cles_inc), flush=True)
for k, n in cles_inc.most_common():
    print("  CLE", k, n, flush=True)
print("ECHECS_CONVERSION tracknumber", tn_echec, flush=True)
echecs, ecartees = [], collections.Counter()
for k, n in sorted(cles_inc.items()):
    pre, nom = k.split(".", 1)
    niv = NIV_DE[pre + "."]
    if nom in INTERDITES or nom in NIV[niv]["purges"]:
        echecs.append("CHAMP_PURGE_OU_INTERDIT %s : %d lignes" % (k, n))
    elif nom in NIV[niv]["ecartes"]:
        ecartees[k] = n
    else:
        echecs.append("CHAMP_NOUVEAU %s : %d lignes" % (k, n))
for m, n in sorted(par_motif.items()):
    if m.startswith("champ_disparu:"):
        echecs.append("CHAMP_DISPARU %s : %d lignes" % (m.split(":", 1)[1], n))
diagnostics = {}
if suspects or par_motif.get("type_non_conforme") or any(m.startswith("type_modifie:") for m in par_motif):
    diagnostics = {r["m"]: r["count"] for r in
                   p2.where((col("motif") == "type_non_conforme") | col("motif").startswith("type_modifie:") | col("suspect"))
                     .select(F.explode(diagnostiquer(col("value"))).alias("m")).groupBy("m").count().collect()}
tm_lignes = sum(n for m, n in par_motif.items() if m.startswith("type_modifie:"))
tm_champs = sorted({c for m in par_motif if m.startswith("type_modifie:") for c in m.split(":", 1)[1].split(",")})
diag_champs = sorted({m.split(" ")[1] for m in diagnostics if m.startswith("TYPE_MODIFIE")})
tm_tolere = tm_lignes > 0 and len(tm_champs) <= TM_CHAMPS_MAX and tm_lignes <= TM_TAUX_MAX * lues
if tm_lignes:
    print("TYPE_MODIFIE lignes", tm_lignes, "champs", tm_champs, "taux", round(tm_lignes / max(1, lues), 8),
          "seuils", TM_TAUX_MAX, TM_CHAMPS_MAX, "TOLERE" if tm_tolere else "REFUSE", flush=True)
if tm_lignes and not tm_tolere:
    for m, n in sorted(diagnostics.items()):
        if m.startswith("TYPE_MODIFIE"):
            echecs.append("%s : %d lignes" % (m, n))
    echecs.append("TYPE_MODIFIE_HORS_SEUIL %d lignes, %d champ(s)" % (tm_lignes, len(tm_champs)))
elif set(diag_champs) - set(tm_champs):
    echecs.append("TYPE_MODIFIE_NON_ROUTE %s" % sorted(set(diag_champs) - set(tm_champs)))
absents = sorted(k for k in presence if presence[k] == 0)
print("SUSPECTS_TYPE", suspects, "DIAGNOSTICS", diagnostics, flush=True)
print("CLES_ECARTEES_VUES", dict(ecartees), flush=True)
print("AVERTISSEMENT_CHAMPS_FACULTATIFS_ABSENTS", len(absents), absents, flush=True)
for e in echecs:
    print("ECHEC_CONTRAT", e, flush=True)
if echecs:
    print("CONTRAT_ECHEC", len(echecs), "aucune ecriture", flush=True)
    sys.exit(1)
print("CONTRAT_OK", flush=True)
if CONTROLE_SEUL:
    ok = lues == attendu
    print("CONTROLE_SEUL", "LUES_OK" if ok else "LUES_ECHEC", "aucune ecriture", flush=True)
    sys.exit(0 if ok else 1)

aplati = p2.where(col("categorie") == "aplati").select(
    col("e.user_id").alias("user_id"),
    col("e.timestamp").alias("timestamp"),
    "date", "mois",
    col("e.recording_msid").alias("recording_msid"),
    col(TMP + "artist_name").alias("artist_name"),
    col(TMP + "track_name").alias("track_name"),
    col(TMP + "release_name").alias("release_name"),
    F.coalesce(col(A + "recording_mbid"), col(A + "track_mbid"), col(M + "recording_mbid")).alias("mbid_enregistrement"),
    F.when(col(A + "recording_mbid").isNotNull(), "recording_mbid")
     .when(col(A + "track_mbid").isNotNull(), "track_mbid")
     .when(col(M + "recording_mbid").isNotNull(), "mbid_mapping").alias("mbid_provenance"),
    F.coalesce(col(A + "artist_mbids"), F.when(col(A + "artist_mbid").isNotNull(), F.array(col(A + "artist_mbid"))),
               col(M + "artist_mbids")).alias("mbids_artistes"),
    F.coalesce(col(A + "release_mbid"), col(M + "release_mbid")).alias("mbid_parution"),
    col(A + "release_group_mbid").alias("mbid_groupe_parution"),
    col(A + "artist_msid").alias("artist_msid"),
    col(A + "release_msid").alias("release_msid"),
    col(A + "artist_names").alias("artist_names"),
    col(A + "release_artist_name").alias("release_artist_name"),
    col(A + "release_artist_names").alias("release_artist_names"),
    col(A + "lastfm_track_mbid").alias("lastfm_track_mbid"),
    col(A + "lastfm_artist_mbid").alias("lastfm_artist_mbid"),
    col(A + "lastfm_release_mbid").alias("lastfm_release_mbid"),
    col(A + "isrc").alias("isrc"),
    dm_int.alias("duration_ms"),
    col(A + "duration").alias("duration_brut"),
    col(A + "track_length").alias("track_length_brut"),
    tn_int.alias("tracknumber"),
    F.coalesce(col(A + "music_service"), col(TMP + "music_service")).alias("music_service"),
    col(A + "music_service_name").alias("music_service_name"),
    F.coalesce(col(TMP + "spotify_id"), col(A + "spotify_id")).alias("spotify_id"),
    col(A + "spotify_album_id").alias("spotify_album_id"),
    col(A + "spotify_artist_ids").alias("spotify_artist_ids"),
    col(A + "spotify_album_artist_ids").alias("spotify_album_artist_ids"),
    col(A + "spotify_track_uri").alias("spotify_track_uri"),
    col(M + "artists").alias("mm_artistes"),
    col(M + "recording_name").alias("mm_recording_name"),
    col(M + "caa_id").alias("mm_caa_id"),
    col(M + "caa_release_mbid").alias("mm_caa_release_mbid"),
)
doublons_lot = 0
if INCR:
    avant = n_aplati
    aplati = aplati.dropDuplicates(["user_id", "timestamp", "recording_msid"]).withColumn("dump", F.lit(DUMP))
    retard = F.datediff(F.lit(RECEPTION).cast("date"), col("date"))
    aplati = aplati.withColumn("jour", F.when(retard <= RETARD_MAX_JOUR, F.date_format("date", "yyyy-MM-dd"))
                                        .otherwise(F.lit("_ancien")))
    comptes = {(r["mois"], r["jour"]): r["count"] for r in aplati.groupBy("mois", "jour").count().collect()}
    n_aplati = sum(comptes.values())
    doublons_lot = avant - n_aplati
    print("DOUBLONS_LOT triplet", doublons_lot, flush=True)
    recents = {k: n for k, n in comptes.items() if k[1] != "_ancien"}
    print("JOURS partitions_jour", len(recents), "ecoutes_recentes", sum(recents.values()),
          "ecoutes_anciennes", n_aplati - sum(recents.values()), "reception", RECEPTION, flush=True)
    plan = {k: max(1, math.ceil(n * OCTETS_PAR_LIGNE / CIBLE_FICHIER)) for k, n in comptes.items()}
    plan_df = spark.createDataFrame([(m, j, n) for (m, j), n in sorted(plan.items())], "mois string, jour string, n int")
    cles_plan = ["mois", "jour"]
else:
    plan = {m: max(1, math.ceil(n * OCTETS_PAR_LIGNE / CIBLE_FICHIER)) for m, n in aplati_mois.items()}
    plan_df = spark.createDataFrame(sorted(plan.items()), "mois string, n int")
    cles_plan = ["mois"]
if n_aplati == 0:
    vide = lues == 0 and attendu == 0 and n_rebut == 0 and n_incognito == 0
    if not vide:
        print("APLATI_VIDE_NON_ATTENDU lues", lues, "attendu", attendu, "rebut", n_rebut, "incognito", n_incognito, flush=True)
        print("APLATI_ECHEC", flush=True)
        sys.exit(1)
    if INCR:
        print("JOURS_INCOHERENTS", 0, flush=True)
    print("SORTIE LIGNES", 0, "MOIS", 0, "REBUT_RELU", 0, "UID_INVALIDES", 0, "DATES_INCOHERENTES", 0, flush=True)
    print("DUMP_VIDE aucune ecriture", flush=True)
    print("MODE", "incremental dump=" + DUMP if INCR else "complet", "DOUBLONS_LOT", doublons_lot, flush=True)
    print("APLATI_OK", flush=True)
    sys.exit(0)
n_seaux = sum(plan.values())
print("PLAN PARTITIONS", len(plan), "FICHIERS_PREVUS", n_seaux, "MAX_PAR_PARTITION", max(plan.values()), flush=True)
(aplati.join(F.broadcast(plan_df), cles_plan)
       .withColumn("seau", F.pmod(F.hash("user_id", "timestamp"), col("n")))
       .repartition(n_seaux, *cles_plan, "seau")
       .drop("n", "seau")
       .write.mode("overwrite").partitionBy(*(["mois", "jour", "dump"] if INCR else ["mois"])).parquet(sortie_aplati))
t_aplati = time.time()

if n_rebut > 0:
    rb = p2.where(col("categorie") == "rebut").select("date", "mois", "motif", "value")
    if INCR:
        rb = rb.withColumn("dump", F.lit(DUMP))
    (rb.repartition("mois").write.mode("overwrite").partitionBy(*(["mois", "dump"] if INCR else ["mois"])).parquet(sortie_rebut))
    rr = spark.read.parquet(sortie_rebut).where(col("mois").isin(sorted(rebut_mois)))
    rebut_relu = (rr.where(col("dump") == DUMP) if INCR else rr).count()
else:
    rebut_relu = 0
t_rebut = time.time()

mois_ecrits = sorted(aplati_mois)
relu = spark.read.parquet(sortie_aplati).where(col("mois").isin(mois_ecrits))
jours_incoherents = 0
if INCR:
    relu = relu.where(col("dump") == DUMP).drop("dump")
    r_ = F.datediff(F.lit(RECEPTION).cast("date"), col("date"))
    jours_incoherents = relu.where(((col("jour") == "_ancien") & (r_ <= RETARD_MAX_JOUR)) |
                                   ((col("jour") != "_ancien") & ((r_ > RETARD_MAX_JOUR) | (col("jour") != F.date_format("date", "yyyy-MM-dd"))))).count()
    print("JOURS_INCOHERENTS", jours_incoherents, flush=True)
    relu = relu.drop("jour")
v = relu.agg(
    F.count(F.lit(1)).alias("lignes"),
    F.countDistinct("mois").alias("mois"),
    F.sum((~col("user_id").rlike("^[0-9a-f]{32}$")).cast("long")).alias("uid_invalides"),
    F.sum((F.to_date(F.from_unixtime("timestamp")) != col("date")).cast("long")).alias("dates_incoherentes"),
    F.sum(col("mbid_enregistrement").isNotNull().cast("long")).alias("avec_mbid"),
    F.sum(col("lastfm_track_mbid").isNotNull().cast("long")).alias("avec_lastfm"),
    F.sum((col("mbid_enregistrement").isNotNull() | col("lastfm_track_mbid").isNotNull()).cast("long")).alias("avec_l_un"),
    F.sum(col("tracknumber").isNotNull().cast("long")).alias("avec_tracknumber"),
).first()
provenance = sorted((r["mbid_provenance"] or "aucune", r["count"])
                    for r in relu.groupBy("mbid_provenance").count().collect())
colonnes = relu.columns
interdites_sortie = sorted(c for c in colonnes if c in INTERDITES or c in AI_ECARTEES)
interdites_cles = sorted(k for k in cles_inc if k.split(".")[-1] in INTERDITES)

chemin = spark._jvm.org.apache.hadoop.fs.Path(sortie_aplati)
fs = chemin.getFileSystem(spark._jsc.hadoopConfiguration())
it = fs.listFiles(chemin, True)
tailles = collections.defaultdict(list)
while it.hasNext():
    s = it.next()
    ch = s.getPath().toString()
    if INCR:
        m = re.search(r"mois=(\d{4}-\d{2})/jour=([^/]+)/dump=" + DUMP + "/", ch)
        cle = (m.group(1), m.group(2)) if m else None
    else:
        m = re.search(r"mois=(\d{4}-\d{2})/", ch)
        cle = m.group(1) if m else None
    if ch.endswith(".parquet") and cle in plan:
        tailles[cle].append(s.getLen())
toutes = [t for l in tailles.values() for t in l]
trop = sum(1 for m in plan if len(tailles.get(m, [])) > plan[m])
t_fin = time.time()

print("SORTIE LIGNES", v["lignes"], "MOIS", v["mois"], "REBUT_RELU", rebut_relu,
      "UID_INVALIDES", v["uid_invalides"], "DATES_INCOHERENTES", v["dates_incoherentes"])
print("PROVENANCE_MBID", provenance)
print("COUVERTURE mbid", v["avec_mbid"], "lastfm", v["avec_lastfm"], "l_un_ou_l_autre", v["avec_l_un"],
      "tracknumber", v["avec_tracknumber"])
if toutes:
    print("FICHIERS", len(toutes), "MIN_MO", round(min(toutes) / 1e6, 1), "MAX_MO", round(max(toutes) / 1e6, 1),
          "TOTAL_OCTETS", sum(toutes), "OCTETS_PAR_LIGNE", round(sum(toutes) / max(1, v["lignes"]), 2),
          "MOIS_AU_DELA_DU_PLAN", trop)
print("COLONNES", len(colonnes), "INTERDITES_EN_SORTIE", interdites_sortie, "CLES_INTERDITES_VUES", interdites_cles)
print("PHASES_S demarrage", round(t_session - t_app, 1), "mesure", round(t_mesure - t_session, 1),
      "ecriture_aplati", round(t_aplati - t_mesure, 1), "rebut", round(t_rebut - t_aplati, 1),
      "verification", round(t_fin - t_rebut, 1))
print("MODE", "incremental dump=" + DUMP if INCR else "complet", "DOUBLONS_LOT", doublons_lot, flush=True)
ok = (lues == attendu and n_aplati > 0 and v["lignes"] == n_aplati and v["mois"] == len(aplati_mois)
      and rebut_relu == n_rebut and v["uid_invalides"] == 0 and v["dates_incoherentes"] == 0 and jours_incoherents == 0
      and not interdites_sortie and not interdites_cles and len(toutes) >= len(aplati_mois)
      and trop == 0 and max(toutes) < 2 * CIBLE_FICHIER)
print("APLATI_OK" if ok else "APLATI_ECHEC")
if not ok:
    sys.exit(1)
