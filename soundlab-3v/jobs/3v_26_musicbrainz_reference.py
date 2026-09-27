import sys, time
t_app = time.time()
from pyspark.sql import SparkSession, functions as F, types as T

source, dest, base, attendus = sys.argv[1:5]
attendus = [int(x) for x in attendus.split(",")]
spark = SparkSession.builder.appName("3v_musicbrainz_reference").getOrCreate()
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

S, LG = T.StringType(), T.LongType()
TABLES = [
    ("mb_canonical_recording", "canonical__canonical_musicbrainz_data", 16, "recording_mbid",
     [("id", LG), ("artist_credit_id", LG), ("artist_mbids", S), ("artist_credit_name", S), ("release_mbid", S),
      ("release_name", S), ("recording_mbid", S), ("recording_name", S), ("combined_lookup", S), ("score", LG)]),
    ("mb_recording_redirect", "canonical__canonical_recording_redirect", 4, "recording_mbid",
     [("recording_mbid", S), ("canonical_recording_mbid", S), ("canonical_release_mbid", S)]),
    ("mb_release_redirect", "canonical__canonical_release_redirect", 4, "release_mbid",
     [("release_mbid", S), ("canonical_release_mbid", S), ("release_group_mbid", S)]),
]
UUID = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
TYPES_GLUE = {"StringType()": "string", "StringType": "string", "LongType()": "bigint", "LongType": "bigint"}

def declarer_table_glue(glue, base, table, emplacement, schema, description):
    from botocore.exceptions import ClientError
    colonnes = [{"Name": c.name, "Type": TYPES_GLUE.get(str(c.dataType), "string")} for c in schema.fields]
    definition = {
        "Name": table, "Description": description, "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE", "projet": "SoundLab", "tache": "3v_2.3",
                       "source": "MusicBrainz canonical dump 20260917, CC0 1.0"},
        "StorageDescriptor": {
            "Columns": colonnes, "Location": emplacement,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                          "Parameters": {"serialization.format": "1"}},
            "Compressed": True,
        },
    }
    try:
        glue.create_table(DatabaseName=base, TableInput=definition)
        return "creee"
    except ClientError as err:
        if err.response["Error"]["Code"] == "AlreadyExistsException":
            glue.update_table(DatabaseName=base, TableInput=definition)
            return "mise_a_jour"
        raise

glue = None
if base != "-":
    import boto3
    glue = boto3.client("glue", region_name="eu-north-1")
ok = True
for (table, prefixe, n_fichiers, cle, colonnes), attendu in zip(TABLES, attendus):
    t0 = time.time()
    schema = T.StructType([T.StructField(n, t) for n, t in colonnes])
    df = (spark.read.schema(schema).option("header", True).option("multiLine", True).option("escape", '"')
          .option("mode", "FAILFAST").csv(f"{source}/{prefixe}/*.csv.zst"))
    sortie = f"{dest}/{table}"
    df.repartition(n_fichiers).write.mode("overwrite").parquet(sortie)
    relu = spark.read.parquet(sortie)
    v = relu.agg(F.count(F.lit(1)).alias("lignes"),
                 F.sum(F.col(cle).isNull().cast("long")).alias("cle_nulle"),
                 F.sum((~F.col(cle).rlike(UUID)).cast("long")).alias("cle_non_uuid"),
                 F.countDistinct(cle).alias("cle_distinctes")).first()
    etat = declarer_table_glue(glue, base, table, sortie, relu.schema,
                               "MusicBrainz canonique : " + prefixe.split("__")[-1]) if glue else "glue_saute"
    t_ok = v["lignes"] == attendu and v["cle_nulle"] == 0 and v["cle_non_uuid"] == 0
    ok = ok and t_ok
    print("TABLE", table, "LIGNES", v["lignes"], "ATTENDU", attendu, "CLE", cle, "NULLES", v["cle_nulle"],
          "NON_UUID", v["cle_non_uuid"], "DISTINCTES", v["cle_distinctes"], "GLUE", etat,
          "OK" if t_ok else "ECHEC", "duree_s", round(time.time() - t0, 1), flush=True)
print("REFERENCE_OK" if ok else "REFERENCE_ECHEC", "duree_s", round(time.time() - t_app, 1))
if not ok:
    sys.exit(1)
