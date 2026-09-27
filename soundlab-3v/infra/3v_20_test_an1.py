import os, sys, shutil, subprocess, tempfile, re
from pyspark.sql import SparkSession, Row

JOB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "jobs", "3v_40_jointure_kaggle_lb.py")
base = tempfile.mkdtemp(prefix="an1_")
spark = SparkSession.builder.master("local[2]").appName("test_an1").getOrCreate()
cur = base + "/cur"
A1 = "0" * 21 + "1"; A2 = "0" * 21 + "2"; A3 = "0" * 21 + "3"; A4 = "0" * 21 + "4"
spark.createDataFrame([
    Row(track_id="K1", name="Song One", artist="Band A", spotify_id=A1, date_ingestion="2026-09-10"),
    Row(track_id="K2", name="Song Two", artist="Band B", spotify_id=A2, date_ingestion="2026-09-10"),
    Row(track_id="K3", name="Dup", artist="Same", spotify_id=A3, date_ingestion="2026-09-10"),
    Row(track_id="K4", name="Dup", artist="Same", spotify_id=A4, date_ingestion="2026-09-10"),
    Row(track_id="K5", name="Café", artist="Zoé", spotify_id=None, date_ingestion="2026-09-10"),
    Row(track_id="K6", name="Mb Only", artist="Band C", spotify_id="x", date_ingestion="2026-09-10"),
    Row(track_id="K6", name="Mb Only", artist="Band C", spotify_id="x", date_ingestion="2026-09-01"),
]).write.parquet(cur + "/music_info")
spark.createDataFrame([Row(track_id=t, is_hit=h) for t, h in
                       [("K1", 1), ("K2", 0), ("K3", 0), ("K4", 1), ("K5", 0), ("K6", 1)]]).write.parquet(cur + "/songs_features_labeled")
spark.createDataFrame([
    Row(recording_mbid="M1", artist_credit_name="Band A", recording_name="Song One", combined_lookup="bandasongone"),
    Row(recording_mbid="M6", artist_credit_name="Band C", recording_name="Mb Only", combined_lookup="bandcmbonly"),
    Row(recording_mbid="M9", artist_credit_name="Other", recording_name="X", combined_lookup="otherx"),
]).write.parquet(cur + "/trois_v/musicbrainz/mb_canonical_recording")
spark.createDataFrame([Row(recording_msid="S6", recording_mbid="M6"), Row(recording_msid="S1", recording_mbid="M1"),
                       Row(recording_msid="S7", recording_mbid="M9")]).write.parquet(cur + "/trois_v/musicbrainz/mb_correspondance_msid")
L = []
def ec(n, msid, art, tit, sp=None, uri=None, mois="2010-05"):
    L.extend([Row(recording_msid=msid, artist_name=art, track_name=tit, spotify_id=sp, spotify_track_uri=uri, user_id="u", mois=mois)] * n)
ec(10, "S1", "Band A", "Song One", sp="https://open.spotify.com/track/" + A1)
ec(5, "S2", "Band B", "Song Two", uri="spotify:track:" + A2, mois="2003-01")
ec(4, "S2b", "band b", "song two!", sp=A1, mois="2016-12")
ec(3, "S3", "Same", "Dup")
ec(2, "S5", "Zoé", "Café", sp="https://open.spotify.com/album/" + A1)
ec(6, "S6", "Band C ", "Mb Only (live)")
ec(7, "S7", "Nobody", "Nothing")
ec(3, "S8", "Band B", "Song Two", mois="2012-02")
spark.createDataFrame(L).write.partitionBy("mois").parquet(base + "/aplati")
spark.stop()

def lancer(attendu, kattendu):
    s = base + "/sortie_%d" % attendu
    p = subprocess.run([sys.executable, JOB, base + "/aplati", cur, s, str(attendu), str(kattendu)],
                       capture_output=True, text=True, env=dict(os.environ, PYSPARK_SUBMIT_ARGS="--master local[2] pyspark-shell"))
    return p.returncode, p.stdout

rc, out = lancer(40, 6)
print(out)
def trouve(motif):
    return re.search(motif, out) is not None
T = [
 ("code 0", rc == 0),
 ("verdict", trouve(r"AN1_OK")),
 ("kaggle dedoublonne 7->6", trouve(r"KAGGLE lignes 7 titres 6 ATTENDU 6")),
 ("cle non ascii 1", trouve(r"cle_non_ascii 1 ")),
 ("cle ambigue 1 (2 titres)", trouve(r"cles_ambigues 1 titres_ambigus 2")),
 ("formats kaggle", trouve(r"'autre': 1, 'absent': 1") or trouve(r"'absent': 1, 'autre': 1")),
 ("hypothese cle 1.0", trouve(r"HYPOTHESE_CLE lignes_ascii 3 accord 3 taux 1.0")),
 ("kaggle vers mb 2", trouve(r"KAGGLE_VERS_MB titres_relies 2 ")),
 ("combos total 40", trouve(r"ECOUTES 40 ATTENDU 40")),
 ("formats LB", trouve(r"'absent': \(24, ") and trouve(r"'url_track': \(10, ") and trouve(r"'nu_22': \(4, ") and trouve(r"'autre': \(2, ")),
 ("voie cle 22 ecoutes 2 titres", trouve(r"VOIE cle_artiste_titre ecoutes_couvertes 22 .* titres_kaggle 2 ")),
 ("voie spotify 19 ecoutes 2 titres", trouve(r"VOIE spotify ecoutes_couvertes 19 .* titres_kaggle 2 ")),
 ("voie mb 16 ecoutes 2 titres", trouve(r"VOIE musicbrainz ecoutes_couvertes 16 .* titres_kaggle 2 ")),
 ("union 28 ecoutes 3 titres", trouve(r"UNION ecoutes_couvertes 28 .* titres_kaggle 3 ")),
 ("accord spotify/cle 15 sur 19", trouve(r"ACCORD spotify_vs_cle ecoutes 19 identiques 15 ")),
 ("accord spotify/mb 10 sur 10", trouve(r"ACCORD spotify_vs_musicbrainz ecoutes 10 identiques 10 ")),
 ("couverture hits 2 sur 3", trouve(r"COUVERTURE_CIBLE is_hit 1 titres 3 trouves 2 ")),
 ("couverture non hits 1 sur 3", trouve(r"COUVERTURE_CIBLE is_hit 0 titres 3 trouves 1 ")),
]
rc2, out2 = lancer(41, 6)
T.append(("attendu faux -> echec", rc2 == 1 and "AN1_ECHEC" in out2))
rc3, out3 = lancer(40, 7)
T.append(("kaggle attendu faux -> echec", rc3 == 1 and "AN1_ECHEC" in out3))
for n, ok in T:
    print("OK  " if ok else "ECHEC", n)
print("TESTS", sum(ok for _, ok in T), "/", len(T))
shutil.rmtree(base)
sys.exit(0 if all(ok for _, ok in T) else 1)
