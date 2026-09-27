# Lambda d'ingestion ListenBrainz (tache 3.3, option B) : portage de ingestion/3v_10 v3, mode incremental.
# Flux : HTTPS -> zstd -> tar -> filtre en LISTE BLANCHE (contrat v1) -> user_id pseudonymise (HMAC-SHA256
# sale, 32 hex ; sel lu dans Secrets Manager, jamais journalise) -> zstd -> envoi S3 multipartie.
# Le dump est ecrit tel quel sous <prefixe>/dump=<n>/part-<n>.json.zst ; le manifeste (comptages seulement)
# sous <prefixe_manifestes>/manifeste_listenbrainz_<n>_incremental.json. Empreinte du flux comparee a la
# valeur publiee : en cas d'ecart, l'envoi est annule et rien n'est ecrit.
import collections, datetime, hashlib, hmac, io, json, os, tarfile, time, urllib.request
import boto3
import zstandard

BASE = "https://data.metabrainz.org/pub/musicbrainz/listenbrainz/incremental"
SECRET = os.environ.get("SECRET_SEL", "soundlab/pseudonymisation-salt-listenbrainz")
UTC = datetime.timezone.utc
PARTIE = 16 * 1024 * 1024
CONTRAT = json.load(open(os.path.join(os.path.dirname(__file__), "config", "3v_contrat_listenbrainz_v1.json"), encoding="utf-8"))
NIV = CONTRAT["niveaux"]
INTERDITES = set(CONTRAT["interdites_toujours"])
PREFIXE = {"racine": "racine.", "tm": "tm.", "ai": "ai.", "mm": "mm.", "artiste": "mm.artists[]."}


def filtrer(obj, niveau, retirees):
    gardes = NIV[niveau]["gardes"]
    out = {}
    for k, v in obj.items():
        d = gardes.get(k)
        if d is None:
            if k in INTERDITES or k in NIV[niveau]["purges"]:
                fam = "interdites_ou_purgees"
            elif k in NIV[niveau]["ecartes"]:
                fam = "ecartees"
            else:
                fam = "inconnues"
            retirees[fam][PREFIXE[niveau] + k] += 1
            continue
        if d["type"] == "objet" and isinstance(v, dict):
            v = filtrer(v, d["niveau"], retirees)
        elif d["type"] == "liste_objet" and isinstance(v, list):
            v = [filtrer(x, d["niveau"], retirees) if isinstance(x, dict) else x for x in v]
        out[k] = v
    return out


class LecteurHache(io.RawIOBase):
    """Enveloppe un flux et calcule son SHA-256 au fil de la lecture."""
    def __init__(self, flux):
        self.flux, self.h, self.n = flux, hashlib.sha256(), 0
    def readable(self):
        return True
    def readinto(self, b):
        d = self.flux.read(len(b))
        self.h.update(d)
        self.n += len(d)
        b[:len(d)] = d
        return len(d)


class EnvoiMultipartie(io.RawIOBase):
    """Flux d'ecriture vers S3 en envoi multipartie (parties de 16 Mio)."""
    def __init__(self, s3, bucket, cle):
        self.s3, self.b, self.k = s3, bucket, cle
        self.id = s3.create_multipart_upload(Bucket=bucket, Key=cle)["UploadId"]
        self.parties, self.tampon, self.octets = [], bytearray(), 0
    def writable(self):
        return True
    def write(self, d):
        self.tampon += d
        self.octets += len(d)
        while len(self.tampon) >= PARTIE:
            self._envoyer(bytes(self.tampon[:PARTIE]))
            del self.tampon[:PARTIE]
        return len(d)
    def _envoyer(self, d):
        n = len(self.parties) + 1
        e = self.s3.upload_part(Bucket=self.b, Key=self.k, UploadId=self.id, PartNumber=n, Body=d)["ETag"]
        self.parties.append({"PartNumber": n, "ETag": e})
    def terminer(self):
        if self.tampon or not self.parties:
            self._envoyer(bytes(self.tampon))
            self.tampon = bytearray()
        self.s3.complete_multipart_upload(Bucket=self.b, Key=self.k, UploadId=self.id,
                                          MultipartUpload={"Parts": self.parties})
    def annuler(self):
        self.s3.abort_multipart_upload(Bucket=self.b, Key=self.k, UploadId=self.id)


def handler(event, context=None):
    t0 = time.time()
    dump = event["dump"]                       # ex. "2674-20260923-000003"
    n = dump.split("-")[0]
    bucket_brut, prefixe = event.get("bucket_brut", "soundlab-raw-558852"), event["prefixe"]
    bucket_man, prefixe_man = event.get("bucket_manifestes", "soundlab-curated-558852"), event["prefixe_manifestes"]
    s3 = event.get("_s3") or boto3.client("s3")
    sm = event.get("_sm") or boto3.client("secretsmanager")
    ouvrir = event.get("_ouvrir") or (lambda u: urllib.request.urlopen(u, timeout=60))
    sel = sm.get_secret_value(SecretId=SECRET)["SecretString"].strip().encode()
    assert len(sel) == 64, "sel de longueur inattendue"
    url = f"{BASE}/listenbrainz-dump-{dump}-incremental/listenbrainz-listens-dump-{dump}-incremental.tar.zst"
    publie = ouvrir(url + ".sha256").read().decode().split()[0]
    cle = f"{prefixe}/dump={n}/part-{n}.json.zst"
    retirees = {"interdites_ou_purgees": collections.Counter(), "ecartees": collections.Counter(),
                "inconnues": collections.Counter()}
    jours, fichiers, bornes = collections.Counter(), [], {}
    json_octets, ecoutes = 0, 0
    sortie = EnvoiMultipartie(s3, bucket_brut, cle)
    try:
        brut = LecteurHache(ouvrir(url))
        comp = zstandard.ZstdCompressor(level=3).stream_writer(sortie, closefd=False)
        lecteur = zstandard.ZstdDecompressor().stream_reader(io.BufferedReader(brut, 1 << 20))
        with tarfile.open(fileobj=lecteur, mode="r|") as t:
            for m in t:
                nom = m.name.rsplit("/", 1)[-1]
                if nom in ("START_TIMESTAMP", "END_TIMESTAMP", "SCHEMA_SEQUENCE"):
                    bornes[nom] = t.extractfile(m).read().decode("utf-8").strip()
                    continue
                if not m.name.endswith(".listens"):
                    continue
                n_fichier = 0
                for ligne in t.extractfile(m):
                    json_octets += len(ligne)
                    d = json.loads(ligne)
                    dt = datetime.datetime.fromtimestamp(d["timestamp"], UTC)
                    d = filtrer(d, "racine", retirees)
                    d["user_id"] = hmac.new(sel, str(d["user_id"]).encode(), hashlib.sha256).hexdigest()[:32]
                    comp.write((json.dumps(d, separators=(",", ":")) + "\n").encode())
                    jours[dt.date().isoformat()] += 1
                    n_fichier += 1
                fichiers.append({"fichier": m.name.split("/", 1)[-1], "ecoutes": n_fichier})
                ecoutes += n_fichier
        while lecteur.read(1 << 20):
            pass
        while brut.read(1 << 20):
            pass
        comp.flush(zstandard.FLUSH_FRAME)
        calcule = brut.h.hexdigest()
        if calcule != publie:
            raise RuntimeError(f"empreinte du flux {calcule[:12]} differente de la valeur publiee {publie[:12]}")
        sortie.terminer()
    except BaseException:
        sortie.annuler()
        raise
    par_mois = collections.defaultdict(lambda: [0, 0])
    for j, c in jours.items():
        par_mois[j[:7]][0] += c
        par_mois[j[:7]][1] += 1
    man = {"dump_id": n, "dump": dump, "dest": f"s3://{bucket_brut}/{cle}", "version_ingestion": "lambda 3v_ingestion v1 liste blanche",
           "mode": "incremental", "contrat_version": CONTRAT["version"], "bornes": bornes, "fichiers": fichiers,
           "mois": [{"mois": k, "ecoutes": v[0], "jours": v[1]} for k, v in sorted(par_mois.items())],
           "ecoutes_total": ecoutes, "jours_distincts": len(jours),
           "premier_jour": min(jours) if jours else None, "dernier_jour": max(jours) if jours else None,
           "octets_json": json_octets, "octets_flux": brut.n, "octets_ecrits": sortie.octets,
           "sha256_flux": calcule, "sha256_publie": publie, "statut": "OK", "duree_s": round(time.time() - t0, 1),
           "cles_retirees": {f: dict(sorted(c.items())) for f, c in retirees.items()}}
    cle_man = f"{prefixe_man}/manifeste_listenbrainz_{n}_incremental.json"
    s3.put_object(Bucket=bucket_man, Key=cle_man, Body=json.dumps(man, indent=2).encode(), ContentType="application/json")
    return {"statut": "OK", "dump": dump, "ecoutes": ecoutes, "jours_distincts": len(jours), "duree_s": man["duree_s"],
            "octets_flux": brut.n, "octets_ecrits": sortie.octets, "manifeste": f"s3://{bucket_man}/{cle_man}",
            "retirees": {f: sum(c.values()) for f, c in retirees.items()}, "inconnues_distinctes": len(retirees["inconnues"])}
