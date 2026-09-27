# 3v_35 (tache 3.2, critere) : les ecoutes d'hier recues aujourd'hui sont-elles dans la partition d'hier ?
# Lecture seule. Args : aplati_incr dump jour_hier(AAAA-MM-JJ) attendu
# Compte les ecoutes du dump datees de <jour_hier> dans TOUTE la table et dans jour=<jour_hier> seule :
# les deux doivent etre egales entre elles et a l'attendu mesure independamment (3v_34).
import sys
from pyspark.sql import SparkSession, functions as F

aplati, dump, hier, attendu = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
spark = SparkSession.builder.appName("3v_35_verif_hier").getOrCreate()
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
col = F.col
d = spark.read.parquet(aplati).where(col("dump") == dump)
datees = d.where(F.date_format("date", "yyyy-MM-dd") == hier)
total_datees = datees.count()
dans_partition = datees.where(col("jour") == hier).count()
ailleurs = {r["jour"]: r["count"] for r in datees.where(col("jour") != hier).groupBy("jour").count().collect()}
intrus = d.where((col("jour") == hier) & (F.date_format("date", "yyyy-MM-dd") != hier)).count()
print("HIER", hier, "dump", dump, "ecoutes_datees", total_datees, "dans_jour", dans_partition,
      "ailleurs", ailleurs, "intrus_dans_jour", intrus, "attendu", attendu, flush=True)
ok = total_datees == dans_partition == attendu and not ailleurs and intrus == 0
print("CRITERE_3_2_OK" if ok else "CRITERE_3_2_ECHEC")
sys.exit(0 if ok else 1)
