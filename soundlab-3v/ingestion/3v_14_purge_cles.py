import argparse, csv, glob, json, os, re, subprocess, sys, tempfile, threading, time
import concurrent.futures as cf

B = "soundlab-raw-558852"
CLES = ["ip_addr", "conn_country", "platform", "offline", "offline_timestamp", "shuffle",
        "spotify_episode_uri", "episode_name", "episode_show_name", "audiobook_title",
        "audiobook_uri", "audiobook_chapter_uri", "audiobook_chapter_title"]
def motif(cles):
    return re.compile(rb'"(' + b"|".join(k.encode() for k in cles) + rb')"\s*:')
MOTIF = motif(CLES)
verrou = threading.Lock()

def cle(j):
    return f"listenbrainz/ecoutes/date={j}/part-2663.json.zst"

def lire(k):
    p = subprocess.run(["bash", "-o", "pipefail", "-c", f"aws s3 cp 's3://{B}/{k}' - | zstd -dc"],
                       capture_output=True, check=True)
    return p.stdout.splitlines()

def nettoyer(lignes):
    sortie, retirees = [], 0
    for l in lignes:
        if MOTIF.search(l):
            o = json.loads(l)
            ai = o["track_metadata"]["additional_info"]
            for k in CLES:
                ai.pop(k, None)
            l2 = json.dumps(o, ensure_ascii=False).encode("utf-8")
            if MOTIF.search(l2):
                raise RuntimeError("cle sensible restante hors additional_info")
            sortie.append(l2)
            retirees += 1
        else:
            sortie.append(l)
    return sortie, retirees

def traiter(j, n_lignes, n_touchees, envoyer):
    k = cle(j)
    lignes = lire(k)
    if len(lignes) != n_lignes:
        raise RuntimeError(f"{j} lignes {len(lignes)} attendu {n_lignes}")
    sortie, r = nettoyer(lignes)
    if r == 0 and envoyer:
        return j, 0
    if r != n_touchees:
        raise RuntimeError(f"{j} nettoyees {r} attendu {n_touchees}")
    if not envoyer:
        return j, r
    fd, tmp = tempfile.mkstemp(suffix=".json.zst")
    os.close(fd)
    try:
        subprocess.run(["zstd", "-q", "-3", "-f", "-o", tmp], input=b"\n".join(sortie) + b"\n", check=True)
        subprocess.run(["aws", "s3", "cp", "--only-show-errors", tmp, f"s3://{B}/{k}"], check=True)
    finally:
        os.remove(tmp)
    relu = lire(k)
    restantes = sum(1 for l in relu if MOTIF.search(l))
    if len(relu) != n_lignes or restantes:
        raise RuntimeError(f"{j} relecture lignes {len(relu)} restantes {restantes}")
    return j, r

def main():
    global CLES, MOTIF
    ap = argparse.ArgumentParser()
    ap.add_argument("--liste", required=True)
    ap.add_argument("--journal", required=True)
    ap.add_argument("--essai", type=int, default=0)
    ap.add_argument("--paralleles", type=int, default=4)
    ap.add_argument("--cles", default=",".join(CLES))
    a = ap.parse_args()
    CLES = a.cles.split(",")
    MOTIF = motif(CLES)
    print("CLES", CLES, flush=True)
    lignes_csv = []
    for f in sorted(glob.glob(a.liste)):
        lignes_csv += list(csv.DictReader(open(f)))
    jours = sorted((r["jour"], int(r["lignes"]), int(r["touchees"])) for r in lignes_csv)
    print("LISTE jours", len(jours), "touchees", sum(t for _, _, t in jours), flush=True)
    faits = set()
    if os.path.exists(a.journal):
        faits = {l.split()[1] for l in open(a.journal) if l.startswith("FAIT ")}
    if a.essai:
        a_faire = jours[:a.essai]
    else:
        a_faire = [x for x in jours if x[0] not in faits]
    print("MODE", "essai" if a.essai else "purge", "deja_faits", len(faits), "a_faire", len(a_faire), flush=True)
    t0 = time.time()
    ok_jours, ok_lignes, erreurs = 0, 0, []
    with cf.ThreadPoolExecutor(max_workers=a.paralleles) as ex:
        futs = {ex.submit(traiter, j, n, t, not a.essai): j for j, n, t in a_faire}
        for fu in cf.as_completed(futs):
            try:
                j, r = fu.result()
            except Exception as e:
                erreurs.append(f"{futs[fu]} {e}")
                for autre in futs:
                    autre.cancel()
                continue
            ok_jours += 1
            ok_lignes += r
            if not a.essai:
                with verrou, open(a.journal, "a") as jf:
                    jf.write(f"FAIT {j} {r}\n")
            if ok_jours % 50 == 0:
                print("AVANCEMENT", ok_jours, "/", len(a_faire), round(time.time() - t0), "s", flush=True)
    print("TRAITES jours", ok_jours, "lignes_nettoyees", ok_lignes, "duree_s", round(time.time() - t0), flush=True)
    for e in erreurs[:5]:
        print("ERREUR", e)
    total_faits = len(faits) + (ok_jours if not a.essai else 0)
    if a.essai:
        print("ESSAI_OK" if not erreurs and ok_jours == len(a_faire) else "ESSAI_ECHEC")
    else:
        print("PURGE_OK" if not erreurs and total_faits == len(jours) else "PURGE_INCOMPLETE", total_faits, "/", len(jours))
    sys.exit(1 if erreurs else 0)

if __name__ == "__main__":
    main()
