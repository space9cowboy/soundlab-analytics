import sys, json
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("3v_test_aplatissement").getOrCreate()
print("SPARK_VERSION", spark.version)
print("ANSI", spark.conf.get("spark.sql.ansi.enabled"))

AI = T.StructType([
    T.StructField("recording_mbid", T.StringType()),
    T.StructField("track_mbid", T.StringType()),
    T.StructField("tracknumber", T.StringType()),
    T.StructField("incognito_mode", T.BooleanType()),
])
MM = T.StructType([
    T.StructField("recording_mbid", T.StringType()),
    T.StructField("artists", T.ArrayType(T.StructType([
        T.StructField("artist_mbid", T.StringType()),
        T.StructField("artist_credit_name", T.StringType()),
        T.StructField("join_phrase", T.StringType()),
    ]))),
])
SCHEMA = T.StructType([
    T.StructField("user_id", T.StringType()),
    T.StructField("timestamp", T.LongType()),
    T.StructField("recording_msid", T.StringType()),
    T.StructField("track_metadata", T.StructType([
        T.StructField("artist_name", T.StringType()),
        T.StructField("track_name", T.StringType()),
        T.StructField("additional_info", AI),
        T.StructField("mbid_mapping", MM),
    ])),
    T.StructField("_rebut", T.StringType()),
])
CONNUES = F.array(*[F.lit(f.name) for f in AI.fields])

def ecoute(i, ai, mm=None, **racine):
    o = {"user_id": "u%031d" % i, "timestamp": 1480000000 + i, "recording_msid": "msid-%d" % i,
         "track_metadata": {"artist_name": "A%d" % i, "track_name": "T%d" % i, "additional_info": ai}}
    if mm is not None:
        o["track_metadata"]["mbid_mapping"] = mm
    o.update(racine)
    return json.dumps(o)

lignes = [
    (1, ecoute(1, {"recording_mbid": "r1", "tracknumber": 7})),
    (2, ecoute(2, {"track_mbid": "t2", "tracknumber": "07"})),
    (3, ecoute(3, {"tracknumber": "3/12"}, mm={"recording_mbid": "m3", "artists": [
        {"artist_mbid": "a1", "artist_credit_name": "X", "join_phrase": " & "},
        {"artist_mbid": "a2", "artist_credit_name": "Y", "join_phrase": ""}]})),
    (4, ecoute(4, {"incognito_mode": True})),
    (5, ecoute(5, {"cle_nouvelle": 1, "origin_url": "http://exemple.invalid/x"})),
    (6, ecoute(6, {}, timestamp="pas_un_nombre")),
    (7, '{"user_id": "u7", "timestamp": '),
]
brut = spark.createDataFrame(lignes, "n int, value string")
opts = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_rebut"}
p = brut.select("n", "value", F.from_json("value", SCHEMA, opts).alias("e"))
ai = "e.track_metadata.additional_info"
mm = "e.track_metadata.mbid_mapping"
cles = F.expr("json_object_keys(get_json_object(value, '$.track_metadata.additional_info'))")
a = p.select(
    "n",
    F.coalesce(F.col(ai + ".recording_mbid"), F.col(ai + ".track_mbid"), F.col(mm + ".recording_mbid")).alias("mbid_enregistrement"),
    F.when(F.col(ai + ".recording_mbid").isNotNull(), "recording_mbid")
     .when(F.col(ai + ".track_mbid").isNotNull(), "track_mbid")
     .when(F.col(mm + ".recording_mbid").isNotNull(), "mbid_mapping").alias("mbid_provenance"),
    F.col(ai + ".tracknumber").alias("tracknumber_texte"),
    F.expr("try_cast(e.track_metadata.additional_info.tracknumber as int)").alias("tracknumber"),
    F.col(mm + ".artists.artist_mbid").alias("mm_artistes_mbid"),
    F.col(ai + ".incognito_mode").alias("incognito"),
    F.array_except(cles, CONNUES).alias("cles_inconnues"),
    F.to_json(F.col(ai)).alias("ai_lu"),
    F.col("e._rebut").alias("_rebut"),
)
r = {x["n"]: x for x in a.collect()}

def controle(nom, obtenu, attendu):
    ok = obtenu == attendu
    print(nom, "OK" if ok else "ECHEC", "obtenu", obtenu, "attendu", attendu)
    return ok

res = [
    controle("H1_TEXTE", [r[i]["tracknumber_texte"] for i in (1, 2, 3)], ["7", "07", "3/12"]),
    controle("H1_ENTIER", [r[i]["tracknumber"] for i in (1, 2, 3)], [7, 7, None]),
    controle("H2_REBUT", sorted(i for i in r if r[i]["_rebut"] is not None), [6, 7]),
    controle("H3_CLES_INCONNUES", sorted(r[5]["cles_inconnues"] or []), ["cle_nouvelle", "origin_url"]),
    controle("H3_SANS_FAUX_POSITIF", sum(len(r[i]["cles_inconnues"] or []) for i in (1, 2, 3, 4)), 0),
    controle("H4_TABLEAU_OBJETS", list(r[3]["mm_artistes_mbid"] or []), ["a1", "a2"]),
    controle("H5_INCOGNITO", sorted(i for i in r if r[i]["incognito"] is True), [4]),
    controle("H6_MBID", [(r[i]["mbid_enregistrement"], r[i]["mbid_provenance"]) for i in (1, 2, 3, 4)],
             [("r1", "recording_mbid"), ("t2", "track_mbid"), ("m3", "mbid_mapping"), (None, None)]),
    controle("H7_MINIMISATION", "origin_url" in (r[5]["ai_lu"] or ""), False),
]
print("APLATISSEMENT_OK" if all(res) else "APLATISSEMENT_ECHEC", sum(res), "/", len(res))
if not all(res):
    sys.exit(1)
