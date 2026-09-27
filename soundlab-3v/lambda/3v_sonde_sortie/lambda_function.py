# Sonde (tache 3.3, option B) : une Lambda hors VPC peut-elle telecharger un dump ListenBrainz ?
# Telecharge la tranche de minuit 2675 (3 195 octets) et son empreinte publiee, compare. Aucune ecriture.
import hashlib, json, time, urllib.request

B = "https://data.metabrainz.org/pub/musicbrainz/listenbrainz/incremental/listenbrainz-dump-2675-20260923-000003-incremental/"
F_ = "listenbrainz-listens-dump-2675-20260923-000003-incremental.tar.zst"


def lire(url):
    with urllib.request.urlopen(url, timeout=20) as r:
        return r.read()


def handler(event, context):
    t0 = time.time()
    try:
        donnees = lire(B + F_)
        publie = lire(B + F_ + ".sha256").decode().split()[0]
        calcule = hashlib.sha256(donnees).hexdigest()
        ok = len(donnees) == 3195 and publie == calcule
        detail = {"octets": len(donnees), "sha_publie": publie[:12], "sha_calcule": calcule[:12]}
    except Exception as e:
        ok, detail = False, {"erreur": f"{type(e).__name__}: {e}"}
    return {"verdict": "SORTIE_INTERNET_OK" if ok else "SORTIE_INTERNET_ECHEC", "duree_s": round(time.time() - t0, 2), **detail}
