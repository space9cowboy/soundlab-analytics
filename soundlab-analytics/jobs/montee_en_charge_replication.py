# =============================================================================
#  SoundLab Analytics — Test de montée en charge : réplication de l'historique
#
#  Source   s3://<raw>/msd/user_listening_history.csv   (9 711 301 lignes)
#  Cible    s3://<raw>/montee_en_charge/x<N>/           (CSV, même format)
#
#  Produit N copies de l'historique d'écoute. Chaque copie reçoit des
#  identifiants d'auditeurs distincts AU MÊME FORMAT que l'original
#  (40 caractères hexadécimaux) : la copie 0 garde les identifiants
#  d'origine, la copie k utilise sha1(user_id + "#" + k).
#
#  Pourquoi ce format : le job 08 doit traiter ces données exactement comme
#  les vraies. Un suffixe du type "_r3" changerait le format et pourrait
#  faire échouer - ou réussir - un contrôle pour une mauvaise raison.
#
#  Données REJOUÉES : elles mesurent le comportement de l'infrastructure
#  quand le volume change, pas la qualité du modèle.
# =============================================================================
import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def arguments():
    p = argparse.ArgumentParser(description="Réplication - test de montée en charge")
    p.add_argument("--source", required=True)
    p.add_argument("--cible", required=True)
    p.add_argument("--facteur", type=int, required=True)
    p.add_argument("--fichiers", type=int, default=0,
                   help="Nombre de fichiers CSV en sortie (0 = 3 par copie)")
    return p.parse_args()


def main():
    args = arguments()
    if args.facteur < 2:
        sys.exit("--facteur doit valoir au moins 2")
    nb_fichiers = args.fichiers or 3 * args.facteur

    spark = SparkSession.builder.appName(f"montee_en_charge_x{args.facteur}").getOrCreate()

    # Lecture en texte, comme le job 08 : aucune conversion à ce stade.
    src = (spark.read.option("header", "true").csv(args.source)
           .select("track_id", "user_id", "playcount"))
    n_src = src.count()
    u_src = src.select("user_id").distinct().count()
    print(f"Source : {n_src} lignes, {u_src} auditeurs", flush=True)

    copies = spark.range(args.facteur).withColumnRenamed("id", "copie")
    rep = (
        src.crossJoin(F.broadcast(copies))
        .withColumn(
            "user_id",
            F.when(F.col("copie") == 0, F.col("user_id"))
             .otherwise(F.sha1(F.concat_ws("#", F.col("user_id"),
                                           F.col("copie").cast("string")))),
        )
        .select("track_id", "user_id", "playcount")
    )

    # CSV non compressé, comme la source : un .csv.gz ne se découpe pas
    # entre exécuteurs et fausserait la mesure du job 08.
    (rep.repartition(nb_fichiers)
        .write.mode("overwrite").option("header", "true").csv(args.cible))

    # Contrôle sur ce qui est ÉCRIT, relu depuis S3 - pas sur le DataFrame.
    relu = spark.read.option("header", "true").csv(args.cible)
    n_out = relu.count()
    u_out = relu.select("user_id").distinct().count()
    n_att = n_src * args.facteur
    u_att = u_src * args.facteur
    print(f"Ecrit  : {n_out} lignes (attendu {n_att}), "
          f"{u_out} auditeurs (attendu {u_att})", flush=True)

    if n_out != n_att or u_out != u_att:
        raise RuntimeError("REPLICATION INVALIDE")
    print(f"REPLICATION VALIDE x{args.facteur}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
