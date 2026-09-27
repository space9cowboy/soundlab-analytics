import argparse, hashlib, json, os, subprocess

CAS = ["temoin", "champ_nouveau", "type_modifie", "champ_disparu", "champ_purge"]
BASE = 1893456000

def ecoute(i):
    return {"user_id": hashlib.sha256(b"fictif-%d" % i).hexdigest()[:32], "timestamp": BASE + 3600 + i * 60,
            "recording_msid": "msid-fictif-%d" % i,
            "track_metadata": {"artist_name": "Artiste %d" % (i % 7), "track_name": "Titre %d" % i,
                               "release_name": "Album %d" % (i % 3),
                               "additional_info": {"duration_ms": 180000 + i, "tracknumber": i % 12 + 1,
                                                   "recording_mbid": "mbid-fictif-%d" % i}}}

def alterer(cas, i, o):
    if i % 10 != 0:
        return o
    ai = o["track_metadata"]["additional_info"]
    if cas == "champ_nouveau":
        ai["champ_nouveau_test"] = "x"
    elif cas == "type_modifie":
        ai["duration_ms"] = "trois minutes"
        o["track_metadata"]["release_name"] = 1234
    elif cas == "champ_disparu":
        del o["track_metadata"]["track_name"]
    elif cas == "champ_purge":
        ai["ip_addr"] = "192.0.2.1"
    return o

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dossier", required=True)
    ap.add_argument("--zst", action="store_true")
    ap.add_argument("--lignes", type=int, default=100)
    a = ap.parse_args()
    for cas in CAS:
        d = os.path.join(a.dossier, cas, "date=2030-01-01")
        os.makedirs(d, exist_ok=True)
        texte = "\n".join(json.dumps(alterer(cas, i, ecoute(i))) for i in range(a.lignes)) + "\n"
        f = os.path.join(d, "part-2663.json")
        open(f, "w").write(texte)
        if a.zst:
            subprocess.run(["zstd", "-q", "-f", "--rm", f, "-o", f + ".zst"], check=True)
        print("FICHIER", cas, a.lignes, "lignes", sum(1 for i in range(a.lignes) if i % 10 == 0) if cas != "temoin" else 0, "alterees")

if __name__ == "__main__":
    main()
