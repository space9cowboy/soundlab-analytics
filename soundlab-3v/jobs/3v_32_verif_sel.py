# 3v_32 (tache 3.1, D0) : les user_id de la zone de transit et de la table aplatie ont-ils ete
# pseudonymises avec le meme sel ? Lecture seule. Args : transit_motif aplati
# Meme sel : les auditeurs actifs avant 2017 et encore actifs en 2026 se retrouvent des deux cotes.
# Sel different : HMAC-SHA256 tronque a 128 bits, recouvrement attendu 0 (collision negligeable).
import sys, time
t_app = time.time()
from pyspark.sql import SparkSession, functions as F

transit, aplati = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.appName("3v_32_verif_sel").getOrCreate()
col = F.col
ut = spark.read.text(transit).select(F.get_json_object("value", "$.user_id").alias("user_id")).distinct().cache()
ua = spark.read.parquet(aplati).select("user_id").distinct().cache()
nt, na = ut.count(), ua.count()
hex_t = ut.where(~col("user_id").rlike("^[0-9a-f]{32}$")).count()
communs = ut.join(ua, "user_id", "left_semi").count()
print("SEL user_id transit", nt, "(non hex32", hex_t, ") | aplati", na, "| communs", communs,
      "| part_des_auditeurs_transit", round(100.0 * communs / nt, 2) if nt else 0, "%", flush=True)
print("VERIF_SEL_OK" if communs > 0 and hex_t == 0 else "VERIF_SEL_ECHEC", "duree_s", round(time.time() - t_app, 1))
