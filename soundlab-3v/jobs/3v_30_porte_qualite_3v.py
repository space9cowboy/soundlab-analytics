#!/usr/bin/env python3
# =============================================================================
#  SoundLab Analytics — chantier « trois V », tache 2.5
#  Porte de qualite des nouvelles natures (JSON brut, table aplatie,
#  referentiel MusicBrainz, etiquettes NC-SA, publication, volumetrie).
#
#  Meme modele que jobs/09_tests_qualite.py (Bloc 6, fige, non modifie) :
#  registre de controles, seuils justifies, rapport JSON, code de sortie 1
#  des qu'un controle BLOQUANT echoue. Controles Q1 a Q8 valides par Loic
#  (tache 2.5). Chaque controle doit echouer sur le jeu degrade.
#  v2 (B8, politique L4 et L6) : Q1_cles_inconnues_ingestion lit les manifestes de
#  l'ingestion en liste blanche (avertissement) ; Q5 incremental agrege par jour de
#  publication (les dumps de minuit sont des tranches contigues, pas des anomalies).
# =============================================================================
import argparse, json, re, statistics, sys
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from pyspark.sql import SparkSession, functions as F

CONFIG_AWS = Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 3, "mode": "standard"})
BLOQUANT, AVERTISSEMENT = "BLOQUANT", "AVERTISSEMENT"
UUID = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

SEUILS = {
    # Q5 mois : ratio au median des 12 mois precedents. Mesure sur les
    # 171 mois : hors 2005-02..2005-07 (changement de regime), max 2,98, min 0,939.
    "q5_mois_ratio_haut": 5.0,
    "q5_mois_ratio_bas": 0.5,
    "q5_mois_fenetre": 12,
    # Q5 increments : 30 dumps du 02 au 26/09/2026. 25 dumps de 195 361 983 a
    # 388 649 913 octets ; 5 dumps de minuit de 2 860 a 3 574 octets, tranches
    # contigues au dump plein (bornes verifiees) : volumes sommes par jour.
    "q5_incr_ratio_haut": 5.0,
    "q5_incr_ratio_bas": 0.2,
    "q5_incr_fenetre": 14,
    "q5_historique_min": 6,
    # Q7 : politique P4 de la tache 2.4.
    "q7_vocabulaire_min_enregistrements": 100,
    # Q8 : k n'est PAS fixe (B6, tache 5.3). La valeur passee en argument est
    # une valeur de test, recopiee telle quelle dans le rapport.
}
COLONNES_APLATI = [
    "user_id", "timestamp", "date", "mois", "recording_msid", "artist_name", "track_name", "release_name",
    "mbid_enregistrement", "mbid_provenance", "mbids_artistes", "mbid_parution", "mbid_groupe_parution",
    "artist_msid", "release_msid", "artist_names", "release_artist_name", "release_artist_names",
    "lastfm_track_mbid", "lastfm_artist_mbid", "lastfm_release_mbid", "isrc", "duration_ms", "duration_brut",
    "track_length_brut", "tracknumber", "music_service", "music_service_name", "spotify_id", "spotify_album_id",
    "spotify_artist_ids", "spotify_album_artist_ids", "spotify_track_uri", "mm_artistes", "mm_recording_name",
    "mm_caa_id", "mm_caa_release_mbid"]


def journal(msg):
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def decouper_s3(uri):
    reste = uri.replace("s3://", "").rstrip("/")
    bucket, _, prefixe = reste.partition("/")
    return bucket, prefixe


class Suite:
    def __init__(self):
        self.resultats = []

    def verifier(self, nom, categorie, severite, attendu, obtenu, ok, note=""):
        self.resultats.append({"controle": nom, "categorie": categorie, "severite": severite, "attendu": attendu,
                               "obtenu": obtenu, "statut": "REUSSI" if ok else "ECHOUE", "note": note})
        marque = "✓" if ok else ("✗" if severite == BLOQUANT else "!")
        print(f"  {marque} [{categorie}] {nom}", flush=True)
        print(f"      attendu : {attendu}", flush=True)
        print(f"      obtenu  : {obtenu}", flush=True)
        return ok

    @property
    def echecs_bloquants(self):
        return [r for r in self.resultats if r["statut"] == "ECHOUE" and r["severite"] == BLOQUANT]

    @property
    def avertissements(self):
        return [r for r in self.resultats if r["statut"] == "ECHOUE" and r["severite"] == AVERTISSEMENT]


def lire_json_s3(s3, uri):
    b, k = decouper_s3(uri)
    return json.loads(s3.get_object(Bucket=b, Key=k)["Body"].read().decode("utf-8"))


def anomalies_volume(serie, fenetre, haut, bas, hist_min):
    """serie : liste ordonnee de (lot, volume). Retourne [(lot, volume, mediane, ratio)] hors bornes."""
    out = []
    for i, (lot, n) in enumerate(serie):
        prec = [x for _, x in serie[max(0, i - fenetre):i]]
        if len(prec) < hist_min:
            continue
        med = statistics.median(prec)
        r = (n / med) if med else float("inf")
        if r > haut or r < bas:
            out.append((lot, n, med, round(r, 4)))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--contrat", required=True)
    ap.add_argument("--brut-lot", required=True, help="motif des fichiers JSON du lot recu")
    ap.add_argument("--aplati", required=True)
    ap.add_argument("--mb", required=True, help="dossier des tables mb_canonical_recording et mb_recording_redirect")
    ap.add_argument("--correspondance", required=True)
    ap.add_argument("--vocabulaire", required=True)
    ap.add_argument("--etiquettes", required=True)
    ap.add_argument("--base", required=True)
    ap.add_argument("--tables-nc-sa", required=True, help="noms Glue separes par des virgules")
    ap.add_argument("--publication", required=True)
    ap.add_argument("--k-test", type=int, required=True)
    ap.add_argument("--volumes", required=True)
    ap.add_argument("--rapport", required=True)
    ap.add_argument("--jeu", required=True, help="etiquette du jeu : sain ou degrade")
    ap.add_argument("--manifestes", required=True, help="manifestes d'ingestion du lot, URI S3 separees par des virgules")
    a = ap.parse_args()

    spark = SparkSession.builder.appName("3v_30_porte_qualite").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    s3 = boto3.client("s3", config=CONFIG_AWS)
    glue = boto3.client("glue", region_name="eu-north-1", config=CONFIG_AWS)
    col = F.col
    suite = Suite()
    stats = {}
    journal(f"PORTE 3V — jeu {a.jeu}")

    # ---------------------------------------------------------------- Q1, Q2
    contrat = lire_json_s3(s3, a.contrat)
    niv = contrat["niveaux"]
    interdites = set(contrat["interdites_toujours"])
    niveaux = [("racine", ""), ("tm", "$.track_metadata"), ("ai", "$.track_metadata.additional_info"),
               ("mm", "$.track_metadata.mbid_mapping")]
    txt = spark.read.text(a.brut_lot)
    def cles_niveau(n, ch):
        k = F.expr(f"json_object_keys(get_json_object(value, '{ch}'))") if ch else F.expr("json_object_keys(value)")
        return F.transform(F.coalesce(k, F.array().cast("array<string>")), lambda x: F.concat(F.lit(n + "."), x))
    cles = F.concat(*[cles_niveau(n, ch) for n, ch in niveaux])
    vues = {r["k"]: r["count"] for r in txt.select(F.explode(cles).alias("k")).groupBy("k").count().collect()}
    lignes_lot = txt.count()
    inconnues, interdites_vues = {}, {}
    for k, n in vues.items():
        pre, nom = k.split(".", 1)
        d = niv[pre]
        if nom in interdites or nom in d["purges"]:
            interdites_vues[k] = n
        elif nom not in d["gardes"] and nom not in d["ecartes"]:
            inconnues[k] = n
    stats.update(lot_lignes=lignes_lot, lot_cles_distinctes=len(vues))
    suite.verifier("Q1_cles_hors_contrat", "json_brut", BLOQUANT, "0 cle hors contrat v1 (B10)",
                   f"{len(inconnues)} cle(s) {dict(sorted(inconnues.items()))} sur {lignes_lot} lignes",
                   lignes_lot > 0 and not inconnues, "lot vide = echec : une porte sur un lot vide ne prouve rien")
    suite.verifier("Q2_cles_interdites_ou_purgees", "json_brut", BLOQUANT, "0 cle interdite ou purgee (B4, R8)",
                   f"{len(interdites_vues)} cle(s) {dict(sorted(interdites_vues.items()))}",
                   lignes_lot > 0 and not interdites_vues)

    # Q1 bis (L4) : cles retirees a l'ingestion, lues dans les manifestes (noms et effectifs).
    inc_ing, sans_comptage = {}, []
    for u in [x for x in a.manifestes.split(",") if x]:
        man = lire_json_s3(s3, u)
        if "cles_retirees" not in man:
            sans_comptage.append(u.rsplit("/", 1)[-1])
            continue
        for k, n in man["cles_retirees"].get("inconnues", {}).items():
            inc_ing[k] = inc_ing.get(k, 0) + n
    stats.update(manifestes=len([x for x in a.manifestes.split(",") if x]))
    suite.verifier("Q1_cles_inconnues_ingestion", "json_brut", AVERTISSEMENT,
                   "0 cle inconnue retiree a l'ingestion (revue humaine avant tout contrat v2)",
                   {"inconnues": dict(sorted(inc_ing.items())), "manifestes_sans_comptage": sans_comptage},
                   bool(a.manifestes) and not inc_ing and not sans_comptage,
                   "cles non stockees (liste blanche B8) : avertissement, pas blocage")

    # ---------------------------------------------------------------- Q3, Q4
    ap_df = spark.read.parquet(a.aplati)
    cols = ap_df.columns
    manquantes = sorted(set(COLONNES_APLATI) - set(cols))
    en_trop = sorted(set(cols) - set(COLONNES_APLATI))
    suite.verifier("Q3_colonnes_aplati", "aplati", BLOQUANT, f"exactement les {len(COLONNES_APLATI)} colonnes du job 3v_24",
                   f"manquantes {manquantes}, en trop {en_trop}", not manquantes and not en_trop,
                   "liste blanche : toute colonne supplementaire, interdite ou non, echoue")
    q4 = ap_df.agg(
        F.count(F.lit(1)).alias("lignes"),
        F.sum((~F.coalesce(col("user_id"), F.lit("")).rlike("^[0-9a-f]{32}$")).cast("long")).alias("uid"),
        F.sum((F.to_date(F.from_unixtime("timestamp")) != col("date")).cast("long")).alias("date"),
        F.sum((F.date_format("date", "yyyy-MM") != col("mois")).cast("long")).alias("mois"),
        F.sum((col("timestamp").isNull() | col("date").isNull()).cast("long")).alias("nuls")).first()
    stats.update(aplati_lignes=q4["lignes"])
    suite.verifier("Q4_identifiants_et_dates_aplati", "aplati", BLOQUANT,
                   "0 user_id hors hexadecimal 32, 0 date ou mois incoherent, 0 horodatage nul",
                   f"lignes {q4['lignes']}, uid {q4['uid']}, date {q4['date']}, mois {q4['mois']}, nuls {q4['nuls']}",
                   q4["lignes"] > 0 and q4["uid"] == q4["date"] == q4["mois"] == q4["nuls"] == 0)

    # ---------------------------------------------------------------- Q5
    vol = lire_json_s3(s3, a.volumes)
    am = anomalies_volume([(x["lot"], x["n"]) for x in vol["mois"]], SEUILS["q5_mois_fenetre"],
                          SEUILS["q5_mois_ratio_haut"], SEUILS["q5_mois_ratio_bas"], SEUILS["q5_historique_min"])
    par_jour = {}
    for x in vol["incrementaux"]:
        j = re.search(r"-(\d{8})-", "-" + x["lot"] + "-").group(1)
        par_jour[j] = par_jour.get(j, 0) + x["n"]
    ai_ = anomalies_volume(sorted(par_jour.items()), SEUILS["q5_incr_fenetre"],
                           SEUILS["q5_incr_ratio_haut"], SEUILS["q5_incr_ratio_bas"], SEUILS["q5_historique_min"])
    stats.update(q5_mois_evalues=len(vol["mois"]), q5_incr_lots=len(vol["incrementaux"]), q5_incr_jours=len(par_jour))
    suite.verifier("Q5_volume_mensuel", "volumetrie", AVERTISSEMENT,
                   f"ratio au median {SEUILS['q5_mois_fenetre']} mois dans [{SEUILS['q5_mois_ratio_bas']}, {SEUILS['q5_mois_ratio_haut']}]",
                   {"hors_bornes": [x[0] for x in am], "detail": am}, len(vol["mois"]) > 0 and not am)
    suite.verifier("Q5_volume_incremental", "volumetrie", AVERTISSEMENT,
                   f"volume par jour de publication, ratio au median {SEUILS['q5_incr_fenetre']} jours dans [{SEUILS['q5_incr_ratio_bas']}, {SEUILS['q5_incr_ratio_haut']}]",
                   {"hors_bornes": [x[0] for x in ai_], "detail": ai_}, len(vol["incrementaux"]) > 0 and not ai_)

    # ---------------------------------------------------------------- Q6
    canon = spark.read.parquet(a.mb + "/mb_canonical_recording")
    redir = spark.read.parquet(a.mb + "/mb_recording_redirect")
    corr = spark.read.parquet(a.correspondance)
    def cle_ok(df, c):
        r = df.agg(F.count(F.lit(1)).alias("n"), F.countDistinct(c).alias("d"),
                   F.sum((~F.coalesce(col(c), F.lit("")).rlike(UUID)).cast("long")).alias("f")).first()
        return r["n"], r["n"] - r["d"], r["f"]
    n1, d1, f1 = cle_ok(canon, "recording_mbid")
    n2, d2, f2 = cle_ok(redir, "recording_mbid")
    suite.verifier("Q6_cles_referentiel_mb", "musicbrainz", BLOQUANT, "cles uniques, au format UUID",
                   f"canonique {n1} lignes, doublons {d1}, non UUID {f1} ; redirection {n2} lignes, doublons {d2}, non UUID {f2}",
                   n1 > 0 and n2 > 0 and d1 == f1 == d2 == f2 == 0)
    c = corr.agg(F.count(F.lit(1)).alias("n"), F.countDistinct("recording_msid").alias("d"),
                 F.sum(((col("part") <= 0) | (col("part") > 1) | col("part").isNull()).cast("long")).alias("p")).first()
    hors = corr.join(canon.select("recording_mbid"), "recording_mbid", "left_anti").count()
    suite.verifier("Q6_correspondance_msid", "musicbrainz", BLOQUANT, "msid unique, 0 MBID hors referentiel, part dans ]0, 1]",
                   f"{c['n']} lignes, doublons {c['n'] - c['d']}, hors referentiel {hors}, part hors bornes {c['p']}",
                   c["n"] > 0 and c["n"] == c["d"] and hors == 0 and c["p"] == 0)

    # ---------------------------------------------------------------- Q7
    params_ko = {}
    for t in [x for x in a.tables_nc_sa.split(",") if x]:
        try:
            p = glue.get_table(DatabaseName=a.base, Name=t)["Table"].get("Parameters", {})
            manque = [k for k, v in (("licence", "CC-BY-NC-SA-3.0-US"), ("usage", "academique-non-diffusable-labels"))
                      if p.get(k) != v]
        except glue.exceptions.EntityNotFoundException:
            manque = ["table_absente"]
        if manque:
            params_ko[t] = manque
    suite.verifier("Q7_licence_tables_nc_sa", "etiquettes_nc_sa", BLOQUANT,
                   "parametres licence et usage presents et exacts (B11)", f"ecarts {params_ko}",
                   bool(a.tables_nc_sa) and not params_ko)
    voc = spark.read.parquet(a.vocabulaire)
    etq = spark.read.parquet(a.etiquettes)
    v = voc.agg(F.count(F.lit(1)).alias("n"), F.countDistinct("etiquette").alias("d"),
                F.sum((col("nb_enregistrements") < SEUILS["q7_vocabulaire_min_enregistrements"]).cast("long")).alias("sous")).first()
    hv = etq.join(voc.select("etiquette"), "etiquette", "left_anti").count()
    suite.verifier("Q7_vocabulaire_etiquettes", "etiquettes_nc_sa", BLOQUANT,
                   f"etiquette unique, >= {SEUILS['q7_vocabulaire_min_enregistrements']} enregistrements, 0 etiquette hors vocabulaire",
                   f"{v['n']} etiquettes, doublons {v['n'] - v['d']}, sous le seuil {v['sous']}, hors vocabulaire {hv}",
                   v["n"] > 0 and v["n"] == v["d"] and v["sous"] == 0 and hv == 0)

    # ---------------------------------------------------------------- Q8
    pub = spark.read.parquet(a.publication)
    q8 = pub.agg(F.count(F.lit(1)).alias("n"),
                 F.sum((F.coalesce(col("nb_auditeurs"), F.lit(0)) < a.k_test).cast("long")).alias("sous")).first()
    suite.verifier("Q8_publication_sous_k", "publication", BLOQUANT, f"0 ligne sous k (k de TEST = {a.k_test}, B7)",
                   f"{q8['n']} lignes, {q8['sous']} sous k", q8["n"] > 0 and q8["sous"] == 0,
                   "k definitif fixe en 5.3 (B6) ; ici seul le mecanisme est prouve")

    # ---------------------------------------------------------------- rapport
    total = len(suite.resultats)
    reussis = sum(1 for r in suite.resultats if r["statut"] == "REUSSI")
    rapport = {"tache": "3v_30_porte_qualite", "jeu": a.jeu, "horodatage_utc": datetime.now(timezone.utc).isoformat(),
               "entrees": vars(a), "seuils": SEUILS,
               "synthese": {"controles": total, "reussis": reussis, "echecs_bloquants": len(suite.echecs_bloquants),
                            "avertissements": len(suite.avertissements)},
               "resultats": suite.resultats, "statistiques": stats}
    rapport["statut"] = "ECHEC" if suite.echecs_bloquants else "SUCCES"
    bucket, prefixe = decouper_s3(a.rapport)
    cle = f"{prefixe}/rapport_{a.jeu}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.json"
    s3.put_object(Bucket=bucket, Key=cle, ContentType="application/json",
                  Body=json.dumps(rapport, indent=2, ensure_ascii=False, default=str).encode())
    journal(f"SYNTHESE — {reussis}/{total} controles reussis, {len(suite.echecs_bloquants)} echec(s) bloquant(s), "
            f"{len(suite.avertissements)} avertissement(s)")
    journal(f"Rapport : s3://{bucket}/{cle}")
    for r in suite.avertissements:
        journal(f"AVERTISSEMENT — {r['controle']}")
    for r in suite.echecs_bloquants:
        journal(f"ECHEC BLOQUANT — {r['controle']}")
    print("PORTE_3V_" + rapport["statut"], "jeu", a.jeu, flush=True)
    spark.stop()
    return 1 if suite.echecs_bloquants else 0


if __name__ == "__main__":
    sys.exit(main())
