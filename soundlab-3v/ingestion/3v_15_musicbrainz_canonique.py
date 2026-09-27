import argparse, json, os, subprocess, sys, tarfile, time

def ouvrir(dest, cle):
    if dest.startswith("s3://"):
        cmd = "zstd -q -3 -c | aws s3 cp --only-show-errors - '%s/%s'" % (dest.rstrip("/"), cle)
    else:
        chemin = os.path.join(dest, cle)
        os.makedirs(os.path.dirname(chemin), exist_ok=True)
        cmd = "zstd -q -3 -c > '%s'" % chemin
    return subprocess.Popen(["bash", "-o", "pipefail", "-c", cmd], stdin=subprocess.PIPE)

def fermer(p, cle):
    p.stdin.close()
    if p.wait() != 0:
        raise RuntimeError("ecriture en echec " + cle)

def decouper(flux, dest, stem, par_part):
    entete = flux.readline()
    if not entete:
        return {"enregistrements": 0, "parts": 0, "octets": 0}
    n_part, n_tot, n_dans, octets, tampon, guillemets = 0, 0, 0, len(entete), b"", 0
    cle = "%s/part-%05d.csv.zst" % (stem, n_part)
    p = ouvrir(dest, cle)
    p.stdin.write(entete)
    for ligne in flux:
        octets += len(ligne)
        tampon += ligne
        guillemets += ligne.count(b'"')
        if guillemets % 2:
            continue
        if n_dans == par_part:
            fermer(p, cle)
            n_part += 1
            n_dans = 0
            cle = "%s/part-%05d.csv.zst" % (stem, n_part)
            p = ouvrir(dest, cle)
            p.stdin.write(entete)
        p.stdin.write(tampon)
        n_dans += 1
        n_tot += 1
        tampon, guillemets = b"", 0
    if tampon:
        raise RuntimeError("enregistrement final incomplet dans " + stem)
    fermer(p, cle)
    return {"enregistrements": n_tot, "parts": n_part + 1, "octets": octets, "entete": entete.decode("utf-8").strip()}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--archive", required=True)
    ap.add_argument("--dest", required=True)
    ap.add_argument("--manifeste", required=True)
    ap.add_argument("--par-part", type=int, default=5000000)
    a = ap.parse_args()
    t0 = time.time()
    zst = subprocess.Popen(["zstd", "-dc", a.archive], stdout=subprocess.PIPE)
    membres = []
    with tarfile.open(fileobj=zst.stdout, mode="r|") as tar:
        for m in tar:
            if not m.isfile():
                continue
            nom = m.name.split("/", 1)[1] if "/" in m.name else m.name
            f = tar.extractfile(m)
            e = {"membre": nom, "octets_tar": m.size}
            if nom.lower().endswith(".csv"):
                stem = os.path.splitext(nom)[0].replace("/", "__")
                e.update(decouper(f, a.dest, stem, a.par_part))
                e["prefixe"] = stem
                if e["octets"] != m.size:
                    raise RuntimeError("octets lus %d != taille tar %d pour %s" % (e["octets"], m.size, nom))
            else:
                p = ouvrir(a.dest, "_meta/%s.zst" % nom.replace("/", "__"))
                p.stdin.write(f.read())
                fermer(p, nom)
            membres.append(e)
            print("MEMBRE", json.dumps(e, ensure_ascii=False), "t_s", round(time.time() - t0), flush=True)
    if zst.wait() != 0:
        raise RuntimeError("zstd en echec")
    man = {"archive": os.path.basename(a.archive), "dest": a.dest, "membres": membres, "duree_s": round(time.time() - t0, 1)}
    json.dump(man, open(a.manifeste, "w"), indent=1, ensure_ascii=False)
    print("INGESTION_OK membres", len(membres), "csv", sum(1 for m in membres if "parts" in m),
          "enregistrements", sum(m.get("enregistrements", 0) for m in membres), "duree_s", man["duree_s"], flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("INGESTION_ECHEC", e, flush=True)
        sys.exit(1)
