# 3v_09 (tache 3.1, D4) : charge un dump incremental de la zone de transit vers aplati_incr.
# 1. lit son manifeste et le registre des chargements (S3) ; 2. controle la continuite des bornes
# START/END avec le dernier dump charge (trou ou chevauchement = arret nomme ; un dump deja charge
# peut etre recharge : idempotence par ecrasement) ; 3. avec --executer, lance 3v_24 --incremental
# et inscrit le resultat au registre.
# v2 (tache 3.2) : transmet --reception (jour de debut de fenetre du manifeste) a 3v_24 v5.
import argparse, json, re, subprocess, sys, tempfile, os

TRANSIT = "s3://soundlab-raw-558852/listenbrainz/incrementaux"
APLATI = "s3://soundlab-raw-558852/listenbrainz/aplati_incr"
REBUT = "s3://soundlab-raw-558852/listenbrainz/rebut_incr"
MANIF = "s3://soundlab-curated-558852/trois_v/_manifestes/listenbrainz/manifeste_listenbrainz_{}_incremental.json"
REGISTRE = "s3://soundlab-curated-558852/trois_v/_etat/chargements_incr.json"
CONTRAT = "s3://soundlab-scripts-558852/trois_v/contrats/3v_contrat_listenbrainz_v1.json"


def lire_s3(uri, defaut=None):
    r = subprocess.run(["aws", "s3", "cp", uri, "-"], capture_output=True, text=True)
    if r.returncode != 0:
        if defaut is not None:
            return defaut
        sys.exit("LECTURE_IMPOSSIBLE " + uri)
    return json.loads(r.stdout)


def continuite(dump, man, registre):
    """Retourne (verdict, detail). Verdicts : PREMIER, SUITE, RECHARGEMENT, TROU, CHEVAUCHEMENT."""
    charges = registre["dumps"]
    if str(dump) in charges:
        return "RECHARGEMENT", "dump deja charge, ecrasement de ses partitions"
    deb, fin = man["bornes"]["START_TIMESTAMP"], man["bornes"]["END_TIMESTAMP"]
    suiv = [int(d) for d in charges if int(d) > int(dump)]
    if suiv:
        n = str(min(suiv))
        deb_n = charges[n]["start"]
        if fin != deb_n:
            return ("TROU" if fin < deb_n else "CHEVAUCHEMENT"), f"fin {fin} / debut du dump suivant {n} {deb_n}"
    prec = [int(d) for d in charges if int(d) < int(dump)]
    if not prec:
        return "PREMIER", "aucun dump anterieur charge"
    p = str(max(prec))
    fin_p = charges[p]["end"]
    if deb == fin_p:
        return "SUITE", f"debut {deb} = fin du dump {p}"
    return ("TROU" if deb > fin_p else "CHEVAUCHEMENT"), f"debut {deb} / fin du dump {p} {fin_p}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", required=True)
    ap.add_argument("--executer", action="store_true")
    a = ap.parse_args()
    man = lire_s3(MANIF.format(a.dump))
    assert man.get("statut") == "OK" and man.get("mode") == "incremental", "manifeste non conforme"
    registre = lire_s3(REGISTRE, defaut={"dumps": {}})
    verdict, detail = continuite(a.dump, man, registre)
    print("CONTINUITE", a.dump, verdict, "-", detail, flush=True)
    if verdict in ("TROU", "CHEVAUCHEMENT"):
        sys.exit(1)
    if not a.executer:
        print("CONTROLE_SEUL : relancer avec --executer pour charger")
        return
    cmd = ["bash", "infra/submit_job.sh", "jobs/3v_24_aplatissement.py", TRANSIT, "-", "-", APLATI, REBUT,
           str(man["ecoutes_total"]), CONTRAT, "--incremental", str(a.dump),
           "--reception", man["bornes"]["START_TIMESTAMP"][:10]]
    r = subprocess.run(cmd, capture_output=True, text=True)
    print(r.stdout[-6000:], flush=True)
    m_id = re.search(r"jobRunId = ([a-z0-9]+)", r.stdout)
    m_l = re.search(r"SORTIE LIGNES (\d+)", r.stdout)
    if r.returncode != 0 or "APLATI_OK" not in r.stdout or not m_l:
        sys.exit("CHARGEMENT_ECHEC " + a.dump)
    registre["dumps"][str(a.dump)] = {"start": man["bornes"]["START_TIMESTAMP"], "end": man["bornes"]["END_TIMESTAMP"],
                                      "ecoutes_manifeste": man["ecoutes_total"], "lignes_aplati": int(m_l.group(1)),
                                      "job": m_id.group(1) if m_id else None, "verdict": verdict}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(registre, f, indent=1)
    subprocess.run(["aws", "s3", "cp", f.name, REGISTRE, "--only-show-errors"], check=True)
    os.unlink(f.name)
    print("CHARGEMENT_OK", a.dump, verdict, "lignes", m_l.group(1))


if __name__ == "__main__":
    main()
