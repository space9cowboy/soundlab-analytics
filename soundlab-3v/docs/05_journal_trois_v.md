# Journal technique — chantier « les trois V »

Journal propre au chantier trois V. Le journal du Bloc 6 (`00_journal_technique.md`) est figé et n'est pas modifié.

## Tâche 0.1 : licences des sources

### Décision

Source retenue : ListenBrainz (écoutes) complétée par MusicBrainz (métadonnées d'artistes). Yambda est écarté pour trois raisons : horodatages relatifs (« delta times, binned into 5s units »), donc pas de partition de date possible ; identifiants entiers anonymisés, donc aucune jointure avec le catalogue Kaggle ; restriction « exclusively for scientific and research purposes » en tension avec sa licence Apache 2.0.

### Preuve de licence ListenBrainz, tirée du dump lui-même

Dump complet 2663 du 15/09/2026. Le fichier `COPYING` a été extrait des 1 048 576 premiers octets de l'archive, sans écrire d'écoute sur disque.

| Contrôle | Résultat |
| --- | --- |
| Taille de `COPYING` | 6 390 octets, égale à la taille déclarée dans l'archive |
| Empreintes SHA-256 distinctes entre le dump et `listenbrainz/db/licenses/COPYING-PublicDomain` du dépôt de code | 1, fichiers identiques |
| Occurrences de « CC0 1.0 Universal » | 1 |
| Occurrences de « noncommercial » (contrôle négatif) | 0 |

### Licences retenues

| Source | Licence | Usage commercial | Preuve |
| --- | --- | --- | --- |
| ListenBrainz, écoutes | CC0 1.0 | Autorisé | Dump, contrôles ci-dessus |
| MusicBrainz, données principales | CC0 | Autorisé | Page officielle ; à contrôler sur le dump (tâche 2.3) |
| MusicBrainz, étiquettes, genres, notes | CC BY-NC-SA 3.0 | Interdit | Page officielle ; concerne la tâche 2.4 |
| Kaggle, catalogue et historique existants | CC BY-NC 4.0 | Interdit | Déjà documenté au Bloc 6 |

Le projet reste donc en régime non commercial, du fait de Kaggle et des étiquettes MusicBrainz.

La CC0 ne lève aucune obligation au titre du RGPD : son § 4 exclut explicitement les droits des tiers, dont la vie privée. Les noms d'utilisateurs ListenBrainz sont publics et directement identifiants. Conséquence actée : pseudonymisation en chemin, avant tout stockage durable, zone brute comprise. L'AIPD (tâche 0.2) est un préalable au premier transfert.

### Faits relevés sur le dump, utiles pour la suite

- Écoutes `.tar.zst` : 245 936 631 999 octets. Dump Spark `.tar` : 235 834 204 160 octets, en Parquet et non en JSON contrairement à la documentation. Le serveur accepte les requêtes partielles.
- Ordre chronologique vérifié de 2002/10 à 2005/4 sur les 200 premiers Mo. Pic anormal en 2005/2 : 1 106 430 406 octets décompressés, contre 53 355 en 2005/1.
- Taux de compression d'au moins 6,2 (borne basse mesurée), soit au moins 1,5 To décompressé pour le dump entier (extrapolation, à mesurer).
- Incrémentaux quotidiens de 228 Mo (22/09) et 237 Mo (23/09), un incrémental de 3 195 octets le 23/09, rétention d'environ un mois.

## Tâche 0.4 : zone brute datée

### Convention de partition

Tout objet ListenBrainz de la zone brute a une clé de la forme `listenbrainz/ecoutes/date=AAAA-MM-JJ/<fichier>`, où la date est celle de l'écoute (les fichiers mensuels du dump sont découpés selon la date d'écoute : 0 écoute hors mois sur 3 200 contrôlées). Toute autre clé sous `listenbrainz/` est refusée : `infra/3v_01_verif_partitions.sh` la signale et sort en erreur. Les noms commençant par `_` sont ignorés par Spark à la lecture.

### Cycle de vie

Règle `lb-brut-31j` sur `listenbrainz/` : expiration à 30 jours, versions non courantes purgées à 1 jour, téléversements incomplets purgés à 1 jour. La règle `retention-rgpd` du Bloc 6 (365 jours, tout le compartiment) est conservée à l'identique ; sur le chevauchement, S3 applique l'expiration la plus courte. Un objet témoin non personnel, `listenbrainz/ecoutes/date=2026-09-24/_temoin_cycle_de_vie.txt`, permettra de constater l'effacement effectif 31 jours après son dépôt.

### Secret

Sel propre à ListenBrainz : `soundlab/pseudonymisation-salt-listenbrainz`, 64 caractères hexadécimaux, chiffré par la même clé KMS que le sel du Bloc 6, distinct de celui-ci (empreintes comparées sans affichage).

## Phase 1 : volume réel (tâches 1.1 à 1.4)

### 1.1 — Rapatriement en flux

Voie retenue : le Mac, après mesure (téléchargement 69,5 Mo/s, envoi vers S3 55,8 Mo/s, traitement Python 65,3 Mo/s de JSON). Aucune NAT, aucune modification de l'application EMR, pseudonymisation hors Spark : l'action A2 ne concerne pas ce chemin.

Première tentative coupée : le serveur ferme une connexion muette au-delà d'un délai compris entre 32,9 s (toléré) et 240 s (coupé). Cause : les 28 premiers mois, quasi vides, occupaient 224 s sans lecture réseau. Correctifs : reprise `--reprendre-apres`, mesure de l'attente de fin de mois, `curl -sS` avec code de sortie. Le garde-fou a tenu : aucune journée partielle écrite.

Tranche ingérée : dump 2663, octets 0 à 59 531 845 631, SHA-256 `c0ee3688f36994281da9af1885e635cae548885f714e57431315540258a8a4a7`, identique après retéléchargement indépendant. 695 656 837 écoutes, 5 120 journées, du 2002-10-01 au 2016-12-31. Manifeste : `data/manifeste_listenbrainz_2663_50gio.json`.

### 1.2 — Volumétrie réelle

| Grandeur | Valeur |
| --- | --- |
| Lignes | 695 656 837 (JSON = texte = manifestes, 171 mois sans écart) |
| Auditeurs | 14 394 |
| Titres (± 1 %) | 28,1 M couples artiste–titre · 49,9 M `recording_msid` · 6,5 M `recording_mbid` |
| Période | 2002-10-01 → 2016-12-31 |

Constat : 66,8 fois moins d'auditeurs que le jeu Kaggle pour 71,6 fois plus d'écoutes. Biais de représentativité à traiter avant la phase 6.

### 1.3 — Montée en charge sur données réelles

| Palier | Lignes | vCPU-h par million |
| --- | --- | --- |
| P1 (2016-12) | 7 664 721 | 0,0450 |
| P2 (2015-11 → 2016-12) | 99 785 982 | 0,00646 |
| P3 (2010-08 → 2016-12) | 485 194 425 | 0,00462 |

Pas de courbe en U : coût fixe plus coût linéaire. Expérience témoin sur le job 08 à 50× : multiplier les partitions de brassage par 5 ne change rien ; retirer le `.cache()` divise le coût par deux (2,568 → 1,379 vCPU-h, deux passages à 3,8 % près). La sur-linéarité observée auparavant venait du cache, pas de la réplication. Le job 08 appartient au rendu figé : non modifié.

### 1.4 — Dimensionnement dérivé du volume

Décisions validées : grain jour, partitionnement par mois ; seuil minimal d'auditeurs appliqué à la diffusion seulement. Job `jobs/3v_23_agregation_mois.py` : fichiers par mois = ⌈ lignes × 0,72 × 43,8 o / 256 Mio ⌉ ; partitions de brassage = max(48, ⌈ lignes × 80 o / 128 Mio ⌉) ; disque = max(20 Go, 20 Go × lignes / 485 194 425). Vérifié sur P3 : 77 fichiers de 164 à 240 Mo au lieu de 2 345 de 6 Mo, 2,061 vCPU-h, disque par défaut.

### Vestiges à nettoyer (aucune suppression sans accord)

| Emplacement | Contenu |
| --- | --- |
| `s3://soundlab-curated-558852/trois_v/_tests/hll/` | test HLL synthétique |
| `s3://soundlab-curated-558852/trois_v/mc_reel/p1/`, `p2/`, `p3/` | agrégats de montée en charge, grain jour |
| `s3://soundlab-curated-558852/trois_v/_rapports/volumetrie_brut_2663/` | rapport de volumétrie |
| `s3://soundlab-curated-558852/montee_en_charge/lh_x50_p240/`, `lh_x50_sanscache/` | sorties des témoins |
| `s3://soundlab-curated-558852/_rapports/montee_en_charge_08_x50_p240/`, `_x50_sanscache/` | rapports des témoins |
| Glue `soundlab_curated.mc_listening_history_x50_p240`, `_x50_sanscache` | tables des témoins |
| `jobs/3v_temoin_08_sans_cache.py` | copie du job 08 sans cache |

## Tâche 2.1 — Ingestion JSON : aplatissement des structures imbriquées

**Relevé de structure** (`ingestion/3v_13_releve_schema.py`, 2 000 lignes du premier jour de chaque mois, 171 mois) : 70 chemins, dont 46 clés sous `additional_info` ; un conflit de type réel (`tracknumber` entier ou texte) ; une même information sous plusieurs noms (`recording_mbid` / `track_mbid` / `mbid_mapping.recording_mbid`) ; un tableau d'objets (`mbid_mapping.artists[]`). Limite : les mois sont des dates d'écoute, pas d'envoi.

**Décisions D1-D3 (validées par Loïc)** : table aplatie en Parquet sous `listenbrainz/aplati/mois=AAAA-MM/` (expiration 30 jours, zone affinée inchangée) ; minimisation par liste blanche, clés hors liste comptées et jamais stockées, écoutes incognito exclues ; un seul nom par information, avec colonne de provenance.

**Test synthétique** (`jobs/3v_01_test_aplatissement.py`) : 9 contrôles sur 9 — un champ texte absorbe un entier JSON, `from_json` alimente la colonne de rebut, `json_object_keys` détecte les clés inconnues.

**Job** `jobs/3v_24_aplatissement.py` et lanceur `infra/3v_04_aplati_tranches.py` (8 tranches d'environ 100 M de lignes, attendu exact par tranche tiré des manifestes) :

| Mesure | Valeur |
|---|---|
| Lignes lues | 695 656 837 (exact) |
| Aplaties / rebut / incognito | 695 640 731 / 0 / 16 106 |
| Jetons invalides, dates incohérentes | 0, 0 |
| Échecs de conversion `tracknumber` | 455 |
| Couverture `mbid` ; `mbid` ou Last.fm | 24,3 % ; 71,4 % |
| Sortie | 433 fichiers, 53,3 Go, 76,6 o/ligne (Parquet zstd) |
| Coût | 14,426 vCPU-h, 5 910 s |

**Écart de minimisation détecté par le comptage des clés inconnues** : `ip_addr` (et 12 clés de l'historique étendu Spotify) sur 54 606 lignes, 1 036 jours, 2011-08 à 2016-12 ; `source_ip` sur 12 lignes, le 27/02/2015. Cause : l'ingestion supprimait une liste noire de champs ; le relevé par échantillon ne les avait pas vus. Purge par `ingestion/3v_14_purge_cles.py` depuis le Mac (le rôle EMR n'écrit pas `ecoutes/`), validée par Loïc ; vérification indépendante par `jobs/3v_25_cles_sensibles_brut.py` : 0 ligne touchée, 695 656 837 lignes conservées. Versions S3 non courantes laissées à l'expiration à 1 jour (contrôle prévu : action B9).

**Droits** : politique distincte `SoundLab3VEcritureDerivesBrut` (écriture sur `aplati/*` et `rebut/*` seulement), vérifiée au simulateur IAM chemin par chemin ; politique du Bloc 6 inchangée.

**Erreurs et leçons** :
- Droits IAM non vérifiés avant D1 : premier lancement en échec d'écriture (0,586 vCPU-h). Leçon : vérifier les droits du rôle avant de choisir un emplacement.
- Mesures affichées en fin de job, donc perdues à l'échec : désormais affichées dès le premier passage.
- Contrôle IAM multi-ressources sans valeur probante (décision agrégée) : refait chemin par chemin.
- `os.killpg` renvoie `EPERM` sur macOS pour un groupe terminé ; `spark.conf.get` refuse une valeur de repli non booléenne.
- Minimisation par liste noire insuffisante face à la variété : l'ingestion doit passer en liste blanche (action B8).

## Tâche 2.2 — Évolution de schéma : contrat et arrêt nommé

**Contrat de schéma v1** (`config/3v_contrat_listenbrainz_v1.json`, publié sous `s3://soundlab-scripts-558852/trois_v/contrats/`) : 51 champs gardés typés (texte, entier, booléen, liste, objet, texte ou entier), 36 champs écartés, 14 purgés, 6 interdits. Le job `3v_24` v3 en déduit son schéma et contrôle avant toute écriture.

| Écart | Réaction (politique validée par Loïc) |
|---|---|
| Champ nouveau | échec, `CHAMP_NOUVEAU <champ> : n lignes` |
| Type modifié d'un champ gardé | échec, `TYPE_MODIFIE <champ> attendu X vu Y` |
| Champ obligatoire disparu | échec, `CHAMP_DISPARU <champ>` |
| Champ purgé ou interdit réapparu | échec, `CHAMP_PURGE_OU_INTERDIT <champ>` |
| Champ facultatif absent du lot | avertissement nommé |

**Démonstration** (5 fichiers synthétiques de 100 lignes, `infra/3v_05_fichiers_alteres.py`) : témoin `APLATI_OK` ; les 4 fichiers altérés échouent en nommant le champ (`ai.champ_nouveau_test`, `ai.duration_ms` et `tm.release_name`, `tm.track_name`, `ai.ip_addr`), sans aucune écriture.

**Non-régression** (mode `controle`, 695 656 837 lignes réelles, 9,574 vCPU-h) : `CONTRAT_OK`, 0 suspect de type, 0 champ facultatif absent.

**Erreur et leçon** : sortie de test placée dans le compartiment des journaux, où le rôle EMR écrit sans pouvoir relire (403 à la vérification). Leçon : un emplacement de sortie doit être lisible par le job qui s'y vérifie. Deux objets fictifs restent sous `logs/trois_v/tests/contrat/temoin/` (vestige).

## Tâche 2.3 — Source relationnelle MusicBrainz

**Source** : dump canonique MusicBrainz du 17/09/2026 (`canonical_data/`, 2 367 116 162 octets, SHA-256 conforme à la valeur publiée). Licence lue dans l'archive (`COPYING`) : CC0 1.0. Écartés : `mbdump.tar.bz2` (7,56 Go, CC0, format PostgreSQL), `mbdump-derived` (tags et notes, CC BY-NC-SA), correspondance MSID→MBID de juin 2020 (`labs/`, licence non vérifiée, option ultérieure).

**Ingestion** (`ingestion/3v_15_musicbrainz_canonique.py`, 45 s) vers `raw/musicbrainz/canonical/20260917/`, puis Parquet et catalogue Glue (`jobs/3v_26_musicbrainz_reference.py`, méthode API du Bloc 6) :

| Table Glue | Lignes | Clé unique |
|---|---|---|
| `mb_canonical_recording` | 32 149 529 | `recording_mbid` |
| `mb_recording_redirect` | 7 887 806 | `recording_mbid` |
| `mb_release_redirect` | 5 772 608 | `release_mbid` |

**Jointure** (`jobs/3v_27_correspondance_musicbrainz.py`, 2,016 vCPU-h) : cascade `mbid` (direct puis redirection) → Last.fm → clé artiste + titre. Hypothèse sur `combined_lookup` (minuscules, `[^a-z0-9_]` retiré) vérifiée à 100 % sur 24 638 069 lignes ASCII ; clé utilisée seulement si ASCII et unique dans le catalogue.

| Méthode | Présent | Résolu | Part en cascade |
|---|---|---|---|
| `mbid` | 168,9 M | 49,4 % (dont 19,8 M par redirection) | 12,0 % |
| Last.fm | 327,7 M | 11,6 % | 5,5 % |
| Clé artiste + titre | 641,4 M | 83,2 % | 60,7 % |

**Couverture : 78,13 % des écoutes** (543 516 151 sur 695 640 731). Table `mb_correspondance_msid` : 24 663 382 `msid` résolus (49,1 % des `msid`), part majoritaire moyenne 0,984.

**Précision** (`jobs/3v_28_precision_correspondance.py`) : sur les 76,8 M écoutes résolues à la fois par `mbid` et par la clé, accord de 98,06 % (Last.fm contre clé : 96,0 %). Réserve : ces écoutes, bien étiquetées, ne représentent pas forcément les autres. Les `mbid` non résolus (3,7 M identifiants, 85,5 M écoutes) ne sont pas des identifiants de parution (1 seul cas) : hypothèse de confusion d'entité réfutée ; cause restante non mesurée (enregistrements supprimés ou absents du dump canonique).

**Erreurs d'estimation** : ingestion estimée à 20-40 min (45 s réelles) ; jointure estimée à 5-10 vCPU-h (2,0 réels).

## Tâche 2.4 — Étiquettes textuelles MusicBrainz

**Sources et licences, lues dans les archives.** `mbdump-derived.tar.bz2` du dump `20260923-002121` (518 825 123 octets, SHA-256 conforme à `SHA256SUMS`) : `COPYING` = CC BY-NC-SA 3.0 US. Décision de Loïc : usage strictement académique (option a). Pour relier les étiquettes aux MBID, `mbdump.tar.bz2` du même dump (7 555 519 494 octets, SHA-256 conforme, CC0 1.0) est lu en flux sans être stocké ; seules les colonnes `id` et `gid` de `recording` sont gardées (40 301 761 lignes, 9 colonnes à la source, 7 min 41 s, `ingestion/3v_16_recording_id_gid.zsh`). Zone brute : 1,04 Go déposés (`musicbrainz/derived/20260923-002121/nc_sa/` avec `COPYING`, `musicbrainz/core/20260923-002121/recording_id_gid/`), chiffrés KMS, métadonnées `licence` et `usage`.

**Hypothèse réfutée.** La colonne `id` de `mb_canonical_recording` n'est pas l'identifiant interne MusicBrainz mais un numéro de ligne (1, 2, … ; `recording_tag` cite des enregistrements jusqu'à 47 788 682). Correspondance `id → gid` contrôlée : 0 doublon d'`id`, 0 doublon de `gid`, 0 ligne mal formée, 0 enregistrement étiqueté sans `gid`.

**Mesures préalables** (`recording_tag`, 7 326 196 associations) : 87 982 votes ≤ 0 (1,2 %), 82,1 % à vote unique, 39 508 étiquettes utilisées sur 244 510, 1 775 couvrant au moins 100 enregistrements. Doublons visibles dès le top 25 (`vgm` / `video game music`, `pop/rock` / `pop rock`).

| Règle | Contenu |
|---|---|
| P1 | associations à vote ≤ 0 exclues |
| P2 | NFKC, minuscules, `/ - _` → espace, `&` → `and`, espaces réduites |
| P3 | synonymes versionnés (`config/3v_synonymes_etiquettes_v1.json`) : `vgm` → `video game music` |
| P4 | vocabulaire : étiquette gardée si ≥ 100 enregistrements canoniques |
| P5 | rattachement au MBID canonique via `mb_recording_redirect`, poids = somme des votes |
| P6 | sorties sous `curated/trois_v/nc_sa/`, tables Glue `mb_nc_sa_*` avec paramètres `licence` et `usage` |

**Résultat** (`jobs/3v_29_etiquettes_musicbrainz.py`, 299 s, 0,559 vCPU-h, `ETIQUETTES_OK`) : 240 630 étiquettes normalisées ; `mb_nc_sa_vocabulaire` = 1 610 étiquettes (517 au-delà de 1 000 enregistrements) ; `mb_nc_sa_etiquettes_enregistrement` = 6 278 677 lignes sur 2 009 934 enregistrements canoniques. P5 rattache 461 187 enregistrements qu'une jointure directe aurait perdus ; 20 318 hors catalogue canonique sont écartés. **Couverture : 59,4 % des écoutes** (413 189 442 sur 695 640 731), 73,65 % des écoutes des `msid` résolus.

**Contrôle des fusions.** 254 étiquettes du vocabulaire regroupent plusieurs noms bruts ; les 103 qui impliquent `/` ou `&` ont été relues une à une : aucune ne réunit deux genres sous un troisième qui leur serait étranger, aucune exception ajoutée. Limites : `/` au sens « ou » des catégories parapluie (`pop/rock`, `folk/world/country`) est fondu dans le genre composé ; les équivalents d'ordre ou de graphie (`hip hop rap` / `rap hip hop`, `r and b` / `r b` / `rnb`, `rock pop` / `pop rock`) restent distincts faute de règle mesurée ; 82 % des associations reposent sur un seul vote.

**Erreurs d'estimation et de méthode.** Extraction estimée à 20-60 min (7 min 41 s) ; job estimé à 1-3 vCPU-h (0,559). Archive `mbdump-derived` supposée présente sur le poste alors que le sondage l'avait lue à distance ; bloc heredoc collé non exécuté, remplacé par un script livré avec empreinte ; interprétation « écriture finie » démentie par la mesure (4 minutes plus tard).

## Tâche 2.5 — Porte de qualité des nouvelles natures

**Principe.** Nouveau job `jobs/3v_30_porte_qualite_3v.py`, sur le modèle de la porte du Bloc 6 (`09_tests_qualite.py`, figée, non modifiée) : registre de contrôles, seuils justifiés, rapport JSON sous `curated/trois_v/_rapports/qualite/`, code de sortie 1 dès qu'un contrôle bloquant échoue. Contrôles validés par Loïc :

| Contrôle | Nature | Sévérité | Objet |
|---|---|---|---|
| Q1 | JSON brut du lot | bloquant | 0 clé hors contrat v1 (B10) |
| Q2 | JSON brut du lot | bloquant | 0 clé interdite ou purgée (B4, R8) |
| Q3 | table aplatie | bloquant | exactement les 37 colonnes du job `3v_24` (liste blanche) |
| Q4 | table aplatie | bloquant | `user_id` hexadécimal 32, date, mois et horodatage cohérents |
| Q5 | volumétrie | avertissement | ratio au médian glissant, mois et incrémentaux |
| Q6 | référentiel MusicBrainz | bloquant | clés uniques UUID ; correspondance sans MBID hors référentiel, `part` dans ]0, 1] |
| Q7 | étiquettes NC-SA | bloquant | paramètres `licence` et `usage` exacts (B11) ; vocabulaire unique, ≥ 100, complet |
| Q8 | publication | bloquant | 0 ligne sous *k* (B7) ; *k* de test = 5, *k* définitif en 5.3 (B6) |

**Seuils de Q5, mesurés.** Mois (manifestes, 171 mois, total 695 656 837 conforme) : ratio au médian des 12 mois précédents compris entre 0,939 et 2,98 hors 2005-02 à 2005-07 ; 2005-02 compte 2 544 740 écoutes contre 141 en 2005-01 (changement de régime puis pic). Seuils : > 5 ou < 0,5. Incrémentaux ListenBrainz : publiés **chaque jour** (et non deux fois par semaine comme l'indiquait le document de reprise) ; sur 30 dumps du 02 au 26/09/2026, 25 pèsent de 195 361 983 à 388 649 913 octets et 5 dumps de minuit (2673, 2675, 2677, 2679, 2681), publiés le même jour qu'un dump plein, de 2 860 à 3 574 octets — dont celui de 3 195 octets du 23/09 (§ 0.1). Ratio des lots normaux au médian des 14 précédents : 0,692 à 1,505. Seuils : > 5 ou < 0,2.

**Démonstration.** Jeu sain : données réelles (lot brut `date=2016-12-3*`, 462 763 lignes ; table aplatie complète, 695 640 731 lignes ; référentiel, correspondance et étiquettes réels), table de publication synthétique, séries de volumes sans les lots anormaux. Jeu dégradé : 10 fichiers synthétiques sous `curated/trois_v/_tests/qualite/degrade/` (clé nouvelle, `user_name` et `tags`, colonne en trop, identifiant en clair, date et mois faux, clé MusicBrainz en double et non UUID, MBID inconnu, `part` nulle, table sans licence et table absente, étiquette en double, sous le seuil et hors vocabulaire, titre sous *k*) et séries de volumes complètes.

| Exécution | Résultat | Durée | vCPU-h |
|---|---|---|---|
| Jeu sain | 11/11 réussis, `PORTE_3V_SUCCES` | 215 s | 0,47 |
| Jeu dégradé | 9 échecs bloquants, 2 avertissements, code 1 | 62 s | 0,076 |

Vérification indépendante (`infra/3v_07_verif_porte.py`) : `VERIF_PORTE_OK` — tous les contrôles bloquants échouent sur le jeu dégradé, et Q5 y signale exactement 2005-02 à 2005-07 et les 5 dumps de minuit, ni plus ni moins. Critère de fin atteint.

**Limites.** *k* reste à fixer (B6, 5.3). Les séries « saines » de Q5 excluent par construction les lots anormaux. Q1 et Q2 portent sur le lot reçu ; l'historique complet a été contrôlé en 2.2 (mode `controle`). Plusieurs défauts coexistent dans chaque fichier dégradé ; chaque compteur y vaut au moins 1 dans le rapport. Les doublons d'écoutes ne sont pas contrôlés, faute de mesure de leur taux.

**Constat annexe.** La partition `listenbrainz/ecoutes/date=2026-09-24/` ne contient qu'un témoin de 67 octets (`_temoin_cycle_de_vie.txt`, tâche 0.4), laissé en place.

**Erreurs.** Défaut de code trouvé au test local : une lambda à deux paramètres dans `F.transform` recevait l'indice au lieu du préfixe, ce qui mettait des clés à `null`. Estimations : jeu dégradé annoncé à 0,2-0,3 vCPU-h (0,076 mesurés) ; document de reprise erroné sur la fréquence des incrémentaux.

## Action B8 — Ingestion ListenBrainz en liste blanche

**Constat de départ.** `ingestion/3v_10` v1 fonctionnait en liste noire : pseudonymisation de `user_id`, retrait de `user_name` et de cinq clés sous `additional_info` seulement ; tout le reste était stocké (d'où `ip_addr` en 2.1). Sur le premier incrémental lu, 22 clés inconnues du contrat v1 apparaissent dès 50 000 lignes, dont `comment` (texte libre) et `tm.tags` (interdite, mais à un niveau que la liste noire ne purgeait pas).

**Politique validée par Loïc (L1 à L6, S1 à S4).** Seules les clés « gardées » du contrat sont écrites, à chaque niveau jusqu'aux éléments de `mbid_mapping.artists` ; les clés écartées ne sont plus stockées dans la zone brute ; les clés retirées sont comptées au manifeste en trois familles (noms et effectifs seulement). Q1 bis de la porte lit ces comptages (avertissement) ; Q5 incrémental agrège par jour de publication. Les dumps de minuit sont des tranches contiguës (fin de 2674 = début de 2675 à la microseconde) et sont ingérés. Un incrémental regroupe les écoutes *reçues* : le dump 2674 couvre 7 534 jours d'écoute, du 13/02/2005 au 23/09/2026 (72 % hors du mois de publication). Il est donc écrit tel quel dans une zone de transit `listenbrainz/incrementaux/dump=<n°>/` (expiration `lb-brut-31j` héritée), la répartition par date étant renvoyée à Spark (3.1, 3.2).

**Tests.** `infra/3v_08_test_liste_blanche.py` : 7 contrôles en mode complet et 6 en mode incrémental, tous réussis. Lancé sur la logique en liste noire, le même test échoue sur 3 contrôles et montre l'adresse IP et le texte libre écrits. Porte `3v_30` v2 : jeu sain 12/12 (0,455 vCPU-h), jeu dégradé 9 échecs bloquants, Q1 bis et Q5 exacts (0,1 vCPU-h), `VERIF_PORTE_OK`.

**Ingestion réelle** (vrai sel, dumps 2674 et 2675, empreintes identiques aux valeurs publiées) :

| Dump | Écoutes | Jours d'écoute | Objet en transit | Durée |
|---|---|---|---|---|
| 2674 | 5 008 271 (égal à la mesure indépendante) | 7 534 | 283 610 070 o | 50 s |
| 2675 | 1 | 1 | 294 o | 1,5 s |

Clés retirées sur 2674 : 10 564 862 occurrences interdites ou purgées, 4 255 744 écartées, 27 193 inconnues (43 clés distinctes, 0,54 % des écoutes). Porte sur ce lot : Q1 et Q2 à 0 sur 5 008 272 lignes, Q1 bis avertit sur les 43 clés, `PORTE_3V_SUCCES` (296 s, 0,603 vCPU-h).

**Nettoyage (S4).** L'écoute de 2675 écrite par erreur sous `ecoutes/date=2026-09-22/` avant la bascule en transit a été effacée par suppression de version (ni version ni marqueur restants), pour ne pas fausser le contrôle B9.

**Erreurs.** Hypothèse non vérifiée : un incrémental organisé par mois d'écoute comme le dump complet (le nom `2026/9.listens` ne le garantissait pas) ; le garde-fou « écoute hors mois » a arrêté la première tentative sans aucune écriture. Estimations : ingestion de 2674 annoncée à 5-20 min puis 1-3 min (50 s mesurées).

**Reste ouvert.** Revue des 43 clés inconnues avant un éventuel contrat v2 (B10) ; répartition des écoutes de transit par date et dédoublonnage (3.1, 3.2) ; contrôle de continuité des bornes START/END entre dumps (3.1).

## Tâche 3.1 — Chargement incrémental daté

**Mesures préalables** (`jobs/3v_31_mesure_recouvrement.py`, 556 s, 1,363 vCPU-h) sur la zone de transit (5 008 272 écoutes) : 0 doublon sur le triplet (`user_id`, horodatage, `recording_msid`), ni dans un dump ni entre dumps ; la paire (`user_id`, horodatage) n'est pas une clé (168 154 groupes, 207 453 lignes « en trop », imports en bloc à la même seconde). 136 mois communs avec la table aplatie du dump complet (528 329 écoutes) : recouvrement 0 sur le triplet, 7 sur (horodatage, `msid`) sans `user_id`. Même sel des deux côtés (`jobs/3v_32_verif_sel.py`) : 5 221 des 24 678 auditeurs de la zone de transit (21,16 %) existent dans la table aplatie (14 394 auditeurs) ; un sel différent donnerait 0.

**Conception validée par Loïc (D0 à D6).**

| Règle | Contenu |
|---|---|
| D1 | table `listenbrainz/aplati_incr/`, partitions `mois=` (date d'écoute) puis `dump=` ; la table du dump complet n'est pas touchée |
| D2 | idempotence par écrasement dynamique des partitions du dump |
| D3 | `jobs/3v_24_aplatissement.py` v4, option `--incremental <n°>` : mêmes contrat, rebut et contrôles ; date tirée de l'horodatage ; triplet dédoublonné dans le lot |
| D4 | `infra/3v_09_charger_incremental.py` : continuité START/END avec les dumps chargés avant et après (trou ou chevauchement = arrêt nommé), registre `curated/trois_v/_etat/chargements_incr.json` |
| D5 | pas de comparaison quotidienne avec la table du dump complet (≈ 1,4 vCPU-h) ; `3v_31` reste disponible ponctuellement |
| D6 | preuve par rechargement et empreinte d'ensemble (`jobs/3v_33_empreinte_incr.py`, avec autotest de détection) |

**Droits.** Politique `SoundLab3VEcritureDerivesBrut` étendue à `aplati_incr/*` et `rebut_incr/*` ; simulation chemin par chemin : 12/12 (8 autorisations, dont les 4 anciennes, et 4 refus `implicitDeny` sur `ecoutes/` et `incrementaux/`).

**Chargement.** Dump 2674 : continuité `PREMIER`, `CONTRAT_OK`, 5 008 271 lues et écrites, 0 rebut, 0 doublon, 253 mois, 0,655 vCPU-h. Dump 2675 : continuité `SUITE` (début = fin de 2674 à la microseconde), 1 ligne.

**Preuve du critère de fin.** Rechargement de 2674 (`RECHARGEMENT`, 0,671 vCPU-h) :

| Mesure | Avant | Après |
|---|---|---|
| Lignes | 5 008 272 | 5 008 272 |
| Triplets distincts | 5 008 272 | 5 008 272 |
| Somme `xxhash64` | −4 738 748 035 334 172 755 873 | identique |
| Fichiers Parquet | 254 | 254 |

L'autotest simulant un double chargement fait bien varier lignes et somme (`DETECTION_OK`). **Le retraitement d'un jour déjà traité ne duplique aucune ligne.**

**Limites.** Un fichier par mois et par dump : 253 petits fichiers pour 2674 (jusqu'à 114 Mo, la plupart presque vides), compactage à prévoir. Aucune écoute `incognito` dans les incrémentaux, contre 16 106 dans le dump complet (deux dumps observés seulement). Le registre ne garde que le dernier verdict d'un dump rechargé. Le contrôle de dates incohérentes est tautologique en mode incrémental (date tirée du même horodatage).

**Erreurs.** Zip du chargement préparé mais non envoyé au premier essai (même oubli qu'en 2.1). Estimations : chargement de 2674 annoncé à 0,3-0,8 vCPU-h (0,655 mesurés, conforme).

## Tâche 3.2 — Données arrivées en retard

**Mesure** (`jobs/3v_34_mesure_retard.py`, 0,1 vCPU-h) du retard entre date d'écoute et jour de réception, dump 2674 (reçu le 22/09) :

| Retard | Écoutes | Part | Jours distincts |
|---|---|---|---|
| négatif | 8 | 0,00 % | 1 |
| 0 jour | 1 283 335 | 25,62 % | 1 |
| 1 jour (« hier ») | 84 738 | 1,69 % | 1 |
| 2 à 7 jours | 16 699 | 0,33 % | 6 |
| 8 à 30 jours | 39 036 | 0,78 % | 23 |
| 31 à 365 jours | 578 847 | 11,56 % | 335 |
| plus d'un an | 3 005 608 | 60,01 % | 7 167 |

Retard médian 668 jours (P90 3 641). Une partition par jour pour tout le dump donnerait 7 534 partitions (médiane 282 écoutes, 1 731 jours sous 100 écoutes). Les 8 retards négatifs portent sur un seul jour : écoutes datées des dernières secondes de la fenêtre de réception (fin le 23/09 à 00:00:03), hypothèse non vérifiée écoute par écoute.

**Décision de Loïc : option C.** Partitions `mois=/jour=/dump=` ; `jour` = date d'écoute si le retard au jour de début de fenêtre (manifeste) est ≤ 30 jours, négatifs compris, sinon `_ancien`. Rupture mesurée à 30 jours : 28,4 % des écoutes sur 31 jours en deçà, le reste étalé sur des milliers de jours. Le jour de réception vient du manifeste : un rechargement produit les mêmes partitions, l'idempotence de la 3.1 est préservée.

**Mise en œuvre.** `jobs/3v_24_aplatissement.py` v5 (`--reception` obligatoire en incrémental, contrôle `JOURS_INCOHERENTS` sur les partitions relues, inférence de type des partitions désactivée) ; `infra/3v_09_charger_incremental.py` v2 (transmet le jour de réception). Ancienne disposition retirée avec l'accord de Loïc : registre archivé sous `chargements_incr_v4_mois_dump.json`, contenu de `aplati_incr/` supprimé (0 objet restant), puis 2674 (`PREMIER`, 0,718 vCPU-h) et 2675 (`SUITE`) rechargés.

**Résultat.** 2674 : 32 partitions `jour=` (1 423 816 écoutes récentes, exactement la somme des tranches ≤ 30 jours mesurées), 3 584 455 écoutes `_ancien`, 284 fichiers, `JOURS_INCOHERENTS 0`, `APLATI_OK`. L'écoute unique de 2675, écoutée le 22/09 et reçue le 23/09, est rangée dans `jour=2026-09-22`.

**Preuve du critère** (`jobs/3v_35_verif_hier.py`) : les 84 738 écoutes de 2674 datées du 21/09 (mesure indépendante de `3v_34`) sont toutes dans `jour=2026-09-21`, aucune ailleurs, aucune écoute étrangère dans cette partition : `CRITERE_3_2_OK`. Empreinte d'ensemble inchangée par le changement de disposition (5 008 272 lignes, somme `xxhash64` −4 738 748 035 334 172 755 873). **Une écoute d'hier reçue aujourd'hui atterrit dans la partition d'hier.**

**Limites.** Le seuil de 30 jours repose sur un seul dump. Les 284 fichiers par dump restent de petite taille pour la plupart (compactage toujours à prévoir). La porte `3v_30` (Q3) attend les colonnes de la table du dump complet et devra être adaptée pour lire `aplati_incr`.

## Tâche 3.3 — Chargement quotidien sans intervention

**Constats.** Le DAG `10_pipeline_soundlab.py` (gelé) n'a pas de planification et les jetons d'Airflow durent 4 h : il ne peut pas porter un chargement quotidien. L'application EMR Serverless, sans VPC, n'a pas d'accès Internet (`jobs/3v_36_test_sortie.py` : délai dépassé) : Spark ne peut pas télécharger les dumps. Une fonction Lambda a cet accès (sonde vérifiée).

**Décision de Loïc : option B, conception E1 à E6.** EventBridge Scheduler déclenche chaque jour à 02:00 UTC une machine Step Functions ; elle planifie les dumps publiés au-delà du dernier chargé, puis, un dump à la fois : ingestion par Lambda, contrôle de continuité, aplatissement `3v_24` sur EMR, inscription au registre. Le premier échec arrête l'exécution : aucun dump postérieur n'est chargé, le registre ne peut pas avoir de trou. Alertes et porte quotidienne hors périmètre (E5).

**Mise en œuvre.**
- Lambda `soundlab-3v-ingestion` : portage de `ingestion/3v_10` v3 (liste blanche, pseudonymisation, empreinte du flux comparée à la valeur publiée). Sur 2674 : 160,8 s, contenu décompressé identique octet pour octet à l'ingestion depuis le Mac.
- Lambda `soundlab-3v-pilotage` (`lambda/3v_pilotage/`, v2) : `planifier` (index public, numéros non supposés contigus : 2671 est absent de la liste publiée), `continuite` (copie de la règle de `3v_09` v2, équivalence vérifiée sur 126 cas), `inscrire` (lit `APLATI_OK`, `MODE` et `SORTIE LIGNES` dans la sortie du pilote Spark, même format de registre que `3v_09`). 26 tests locaux (`infra/3v_11_test_pilotage.py`) ; chaque mutation volontaire du code fait échouer au moins un test ; essai réel des trois actions sans écriture (registre inchangé).
- Appel EMR repris des runs réels de 2674 et 2675 : 11 arguments, paramètres Spark par défaut dont `partitionOverwriteMode=dynamic`. Forme de la sortie de `startJobRun.sync`, non documentée, mesurée : `JobRunId` au premier niveau.
- Rôles `SoundLab3VLambdaPilotage` (17/17), `SoundLab3VStepFunctionsChargement` (14/14), `SoundLab3VSchedulerChargement` (4/4), simulés chemin par chemin avec des cas refusés. Piège relevé : en zsh, `$COMPTE:stateMachine` applique le modificateur `:s` ; toutes les variables des scripts sont entre accolades.
- Machine `soundlab-3v-chargement-incremental` (`infra/3v_13_machine_chargement.json`, validée par AWS), planification `soundlab-3v-chargement-quotidien`, `cron(0 2 * * ? *)` UTC.

**Premier essai et incident.** Déclenché par une planification unique du Scheduler : 2676 à 2679 chargés (ingestion de 2676 : 131 s), arrêt sur 2680 : `CONTRAT_ECHEC`, `TYPE_MODIFIE ai.music_service attendu texte vu booleen`, 1 ligne sur 6 354 984, aucune écriture (0 fichier `dump=2680`), 0,526 vCPU-h. Spark convertit silencieusement le booléen en texte ; seul le repérage des lignes suspectes l'avait détecté. Planification quotidienne désactivée le temps du correctif.

**Décision de Loïc : rebut sous seuil.** `jobs/3v_24_aplatissement.py` v6 : une ligne dont un champ gardé a changé de type part au rebut avec le motif `type_modifie:<champ>`, si ces lignes restent ≤ 0,01 % des lignes lues et touchent au plus un champ ; au-delà, échec du contrat sans écriture, comme en v5. 7 tests Spark locaux (`infra/3v_16_test_type_modifie.py`, Spark 3.5.8) dont la v5 sur le même lot (échec attendu) ; doubler les seuils fait échouer les deux cas hors seuil. v5 archivée sous `s3://soundlab-scripts-558852/jobs/_archives/`.

**Preuve du critère.** Essai 2, déclenché par le Scheduler sans intervention : 2680 et 2681 chargés en 24 min 30 s (ingestion de 2680 : 228 s). 2680 : `LUES 6354984 ATTENDU 6354984`, 1 ligne au rebut `type_modifie:ai.music_service` (taux 1,6e-07, `TOLERE`), `CONTRAT_OK`, `JOURS_INCOHERENTS 0`, `SORTIE LIGNES 6354983`, `REBUT_RELU 1`, `APLATI_OK`, 1,414 vCPU-h. Exécution lancée aussitôt après : `RIEN_A_CHARGER` en 0,7 s, ni ingestion ni job EMR, registre inchangé. Registre : 2674 à 2681, tous `SUITE` après 2674, début de chaque dump égal à la fin du précédent. **Un dump publié est chargé sans intervention ; une exécution sans nouveau dump ne fait rien.**

**Limites.** Le déclenchement par l'expression quotidienne (02:00 UTC) a été constaté ensuite (section « Tâche 3.3 (suite) »). Alerte partielle : un échec du job EMR déclenche l'alarme héritée du Bloc 6 `soundlab-emr-jobs-en-echec` (`FailedJobs` ≥ 1 sur l'application, courriel par la rubrique SNS `soundlab-alertes`), constaté sur 2680 : `ALARM` une minute après l'échec, notifications délivrées 1 puis 1 (alarme puis retour), 0 en échec. Un échec du Lambda d'ingestion, une rupture de continuité ou un déclenchement manqué n'alertent pas (E5). Le seuil de tolérance repose sur un seul cas réel. Coût de 2680 en v6 environ 1,6 fois celui de 2674 en v5 par écoute ; hypothèse non vérifiée : la fonction Python reçoit une valeur (nulle pour la plupart) sur chaque ligne et la table intermédiaire est recalculée à chaque passage. Compactage d'`aplati_incr` et adaptation de la porte `3v_30` toujours à faire.

## Analyse des données trois V — tâches AN1 à AN5

**Question.** Les écoutes ListenBrainz changent-elles la cible et la performance du modèle du Bloc 6 ? Même modèle (forêt aléatoire de 300 arbres, graine 42), même protocole ; aucun livrable figé modifié. Sorties sous `curated/trois_v/analyse/an1/` : identifiants de titres et comptages, aucune donnée d'auditeur.

### AN1 — Jointure catalogue Kaggle ↔ écoutes ListenBrainz

`jobs/3v_40_jointure_kaggle_lb.py` (test `infra/3v_20_test_an1.py` 20/20), 547 s, 1,401 vCPU-h. Contrôles : 50 683 titres, 695 640 731 écoutes, règle de clé 24 638 069 / 24 638 069.

| Voie | Écoutes couvertes | Titres Kaggle retrouvés |
|---|---|---|
| Artiste + titre (règle de `combined_lookup`) | 134 549 679 (19,34 %) | 48 565 (95,82 %) |
| MusicBrainz (`mb_correspondance_msid`) | 134 189 936 (19,29 %) | 47 695 (94,10 %) |
| Identifiant Spotify | 43 452 (0,01 %) | 5 386 (10,63 %) |
| Union | 135 259 030 (19,44 %) | 48 671 (96,03 %) |

Accord : Spotify / clé 99,99 % sur 40 727 écoutes, Spotify / MusicBrainz 99,99 % ; MusicBrainz / clé 100,00 %, non probant (même clé côté Kaggle). Spotify n'est présent que sur 0,19 % des écoutes, mais c'est la seule voie indépendante. Couverture selon la cible Kaggle : 97,11 % des succès, 95,95 % des non-succès ; **hypothèse de biais de sélection réfutée** (écart de 1,16 point). 19 356 titres sans écoute Kaggle ont des écoutes ListenBrainz. Les 80 % d'écoutes non couvertes portent sur des titres hors catalogue Kaggle.

### AN3 (et AN2 en partie) — Cible ListenBrainz et concordance

`jobs/3v_41_cible_listenbrainz.py` (test `infra/3v_21_test_an3.py` 20/20), 58 s. La méthode (quantile linéaire, règle ≥) reproduit exactement la cible Kaggle : seuil 592, 7 617 / 7 617 positifs.

| Écoutes par titre (29 315 titres communs) | P10 | P50 | P75 | P90 | Part du top 1 % des titres |
|---|---|---|---|---|---|
| Kaggle `total_plays` | 7 | 137 | 601 | 1 844 | 27,88 % |
| ListenBrainz | 366 | 1 337 | 2 802 | 5 730 | 11,27 % |

Cible ListenBrainz : seuil 2 802, 7 330 positifs (25,0 %). Matrice : 3 215 succès dans les deux sources, 4 182 seulement Kaggle, 4 115 seulement ListenBrainz, 17 803 dans aucune ; accord 0,717, **kappa de Cohen 0,2476**, **Spearman 0,2571**. Variante sur les 48 671 titres trouvés : seuil 3 029, kappa 0,2415 sur les communs, 5 402 nouveaux positifs sur 19 356. Table `an3_cible_par_titre` : 48 671 lignes. **Hypothèse réfutée** (kappa attendu 0,4 à 0,7, déduit à tort d'un rapport de médianes) : deux mesures réelles de la popularité ne classent pas les titres de la même façon. Non mesuré : auditeurs distincts par titre et répartition par année (AN2 partielle).

### AN5 — Même modèle, deux cibles

`ml/3v_50_reentrainement_cible_lb.py` (test `infra/3v_22_test_an5.py` 11/11), protocole de `ml/12` relu : validation croisée groupée par artiste, 5 plis. Variables A (13 audio) et B (+ 3 de contexte) seulement : les variables d'artiste de la variante C dérivent de la cible Kaggle. Témoin : variante B du Bloc 6 rejouée à 0,658 contre 0,6585 publié.

| Variante (29 315 titres communs) | Cible Kaggle | Cible ListenBrainz |
|---|---|---|
| A — audio | 0,6058 ± 0,0153 | 0,5794 ± 0,0063 |
| B — audio + contexte | 0,6624 ± 0,0139 | 0,7179 ± 0,0166 |

**Hypothèse réfutée pour la variante B** : avec le contexte, la cible ListenBrainz se prédit mieux (+0,056).

**Contrôle AN5-b** (`ml/3v_51_controle_contexte.py`, test `infra/3v_23_test_an5b.py` 8/8 ; référence reproduite à l'identique) :

| Configuration | Cible Kaggle | Cible ListenBrainz |
|---|---|---|
| Sans ancienneté | 0,6487 ± 0,0154 | 0,7012 ± 0,0146 |
| Sans taille du catalogue de l'artiste | 0,6209 ± 0,0142 | 0,5907 ± 0,0108 |
| Sans les deux | 0,6050 ± 0,0155 | 0,5765 ± 0,0061 |
| Ancienneté seule | 0,5493 ± 0,0105 | 0,5340 ± 0,0132 |

Spearman : ancienneté / écoutes ListenBrainz −0,0272 ; taille du catalogue / écoutes ListenBrainz 0,3947 (Kaggle : 0,1546). **Soupçon d'effet d'exposition réfuté** : le gain vient de la taille du catalogue de l'artiste (−0,127 sans elle). Pas de fuite : variable calculée sur le catalogue Kaggle, plis groupés par artiste.

### Conclusions

1. La jointure est fiable et non biaisée vers les succès : 96,03 % des titres Kaggle retrouvés.
2. La cible dépend de la source : κ = 0,25 entre deux mesures réelles de popularité. L'AUC de 0,7875 du Bloc 6 est relative à la mesure Kaggle, pas une propriété du modèle.
3. Le son prédit mal le succès quelle que soit la source (0,58 à 0,61) ; l'artiste le prédit, plus encore sur ListenBrainz. La recommandation M2 du rapport de modélisation est confirmée par une source indépendante.

### Limites

Auditeurs distincts et représentativité mesurés ensuite (sous-section AN2 et AN4 ci-dessous) ; la clé artiste + titre regroupe les versions d'un même morceau côté ListenBrainz ; une seule graine ; évaluation temporelle (phase 6) non faite. Mécanisme de la dépendance à l'artiste (écoute par discographie) supposé, non mesuré.

**Erreurs.** Deux hypothèses annoncées puis réfutées par leur propre mesure (kappa, effet d'âge). Deux tests locaux trop faibles corrigés avant livraison : une mutation (voie clé retirée, puis cible LB remplacée par la cible Kaggle) passait inaperçue. Estimation AN1 : 1,5 à 2,5 vCPU-h annoncés, 1,401 mesurés.

### AN2 et AN4 — Auditeurs distincts et représentativité

`jobs/3v_42_auditeurs_representativite.py` (test de bout en bout `infra/3v_25_test_an24.py` 15/15), 586 s, 1,514 vCPU-h. Contrôles : correspondance d'AN1 recalculée sans ambiguïté, 695 640 731 écoutes dont 135 259 030 couvertes, 29 315 titres communs, cible volume d'AN3 reproduite (seuil 2 802, κ 0,2476). Sortie `an2_auditeurs_par_titre` (48 671 lignes, sans jeton).

**Auditeurs.** Écoutes par auditeur, toutes écoutes (14 394 auditeurs) : médiane 24 638, P99 314 523, maximum 1 118 005 ; le 1 % le plus actif produit 9,73 % des écoutes (10,33 % des écoutes couvertes). Par titre commun : médiane 271 auditeurs distincts (P75 509, P99 2 389) et 4,96 écoutes par auditeur. H2 (top 1 % au-delà de 10 %) **non tranchée** : 9,73 % tombe entre les deux seuils annoncés.

**Années d'écoute.** Écoutes couvertes quasi nulles avant 2005 (1 062) ; de 2,3 M en 2005 à un plateau d'environ 13,5 M par an de 2009 à 2016 ; auditeurs actifs de 1 317 (2005) à 11 584 (2016).

**Cible fondée sur les auditeurs distincts.** Seuil P75 = 509 auditeurs, 7 333 positifs ; matrice 3 183 / 4 214 / 4 150 / 17 768 ; **κ = 0,2417**. Spearman auditeurs ListenBrainz / `unique_listeners` Kaggle 0,2518 ; auditeurs / écoutes ListenBrainz 0,9465. **H1 réfutée** (κ attendu au-delà de 0,35) : le désaccord d'AN3 ne vient pas de quelques gros auditeurs ; volume et audience classent les titres presque de la même façon côté ListenBrainz.

**Représentativité (AN4), parts d'écoutes sur les titres communs, distance de variation totale (TVD) :**

| Dimension | TVD | Sur-représenté dans ListenBrainz | Sous-représenté |
|---|---|---|---|
| Décennie de sortie | 0,0704 | 1960s ×3,0 ; 1970s ×2,0 ; 1980s ×1,5 ; 1990s ×1,3 | 2000s ×0,89 |
| Genre (16 modalités, absent compris) | 0,1425 | metal ×1,87 ; blues ×1,83 ; jazz ×1,67 ; punk ×1,67 | country ×0,28 ; latin ×0,29 ; pop ×0,43 ; rap ×0,51 |
| Artiste (5 947) | 0,4204 | artistes rock et indépendants, artistes récents | country et pop grand public (Justin Bieber ×0,02, David Guetta ×0,02) |

Spearman des parts par artiste (1 772 artistes d'au moins 5 titres) : 0,6374. Les rapports extrêmes par artiste (jusqu'à ×158) portent sur des parts Kaggle proches de zéro et sont instables ; seuls la TVD et le Spearman sont à citer.

**Lecture.** Le désaccord entre les deux cibles s'explique par la composition des publics, pas par la mesure : ListenBrainz sur-représente le rock, le metal et les catalogues anciens, et sous-représente la country, la pop et le rap grand public. Hypothèse non mesurée : un décalage de période entre les deux sources (plusieurs artistes sur-représentés ont percé après 2010).

**Erreur de gouvernance.** Les deux tables intermédiaires de `3v_42` (`_tmp_an2_titre_jeton`, `_tmp_an2_jeton`) portaient des jetons pseudonymisés et ont été écrites en zone affinée (rétention 730 jours), contre l'amendement n° 1. Supprimées avec l'accord de Loïc (416 objets, versions comprises, 0 restant) ; consigné dans l'AIPD (amendement n° 9, action B13). Leçon : une table intermédiaire à jetons va dans le compartiment brut, sous l'expiration à 30 jours.

## Tâche 3.3 (suite) — Exécution planifiée et dump vide

**Constat.** Planification `ENABLED`, `cron(0 2 * * ? *)`. Exécution `d16ab878-a051-4c22-a280-209a92d7b8b6` démarrée par le planificateur à l'heure prévue (27 s après 02:00 UTC), sans intervention. Plan : 2682 et 2683. Dump 2682 chargé (job EMR `SUCCESS`, 5 055 339 lignes, inscrit au registre, début égal à la fin de 2681). Échec sur 2683, 15 min 12 s après le démarrage.

**Cause.** 2683 est une tranche de minuit **vide** : 0 écoute au manifeste, `LUES 0 ATTENDU 0`, `CONTRAT_OK`, 0 rebut, 0 incognito. La v6 plantait sur le calcul du plan d'écriture (`max()` sur un plan vide) et exigeait de toute façon au moins une ligne écrite. Défaut de conception : le dump vide n'avait pas été prévu. Anomalie de la source : la fenêtre publiée pour 2683 a une durée négative (fin `00:00:03.401226`, début `00:00:03.412365`) ; son début raccorde bien la fin de 2682.

**Correctif.** `jobs/3v_24_aplatissement.py` v7 : un dump vide (0 ligne lue, 0 attendue, contrat respecté) n'écrit rien et produit `SORTIE LIGNES 0`, `DUMP_VIDE`, `APLATI_OK` ; tout autre lot sans ligne à écrire échoue (`APLATI_VIDE_NON_ATTENDU`). Seul un bloc gardé est ajouté ; le reste de la v6 est inchangé. Tests `infra/3v_28_test_dump_vide.py` en Spark local, 5/5 : lot vide accepté par la v7 et refusé par la v6 (témoin) ; 3 écoutes toutes incognito refusées ; lot vide annoncé à 1 écoute refusé ; lot normal de 2 000 écoutes identique en v6 et v7 (fichiers et empreinte). Une mutation qui accepte tout lot vide fait échouer 2 tests. Le Lambda de pilotage accepte déjà 0 ligne (part des écoutes récentes à 0). v6 archivée sous `jobs/_archives/3v_24_aplatissement_v6.py`, empreinte vérifiée avant et après publication.

**Reprise.** Exécution manuelle `reprise-2683-v7-20260927` : `SUCCEEDED` en 3 min 9 s. Registre : 10 entrées, 2674 à 2683, aucune rupture ; 2683 inscrit avec 0 ligne. **Critère de la tâche 3.3 atteint** : la planification déclenche seule et charge sans intervention ; le seul échec venait d'un cas limite de la source, arrêté sans écriture, diagnostiqué, corrigé et testé. La limite « déclenchement à constater » de la section 3.3 est levée.

**Effacement effectif (action B9).** Au premier contrôle, 1 038 versions non courantes subsistaient sous `listenbrainz/ecoutes/` (1 037 jours, dont les 1 036 jours touchés par `ip_addr` et le 27/02/2015 par `source_ip`), avec la règle `lb-brut-31j` active et correcte. Les plus anciennes sont déjà éligibles : S3 applique l'expiration en différé. Un nouveau contrôle est prévu ; suppression manuelle des versions non courantes s'il en reste.

**Erreurs.** Cas du dump vide non prévu en conception ni en test. Échéance de B9 fixée sans tenir compte du délai d'application des règles de cycle de vie. Commande de comptage fausse (`length()` sur une liste vide renvoyée `null`), remplacée par un comptage Python avec contre-épreuve.

## Tâche 5.1 — Fraîcheur des données

**Définition.** Lambda `soundlab-3v-pilotage` v3, action `inscrire` : avant d'écrire le registre, publication dans l'espace CloudWatch `SoundLab/3V` (dimension `Source=ListenBrainz`) de `FraicheurDisponibiliteSecondes` (heure d'inscription moins `END_TIMESTAMP` du dump), `PartEcoutesRecentesPourcent` (écoutes rangées en `jour=`, retard ≤ 30 jours) et `EcoutesChargees`. Tests `infra/3v_11` v3 32/32 ; rôle de pilotage v2 (`PutMetricData` limité à `SoundLab/3V`) 19/19 par simulation ; tableau `soundlab-3v` (5 widgets, période 60 s, axe en heures) ; tableau du Bloc 6 non modifié.

**Reprise des 6 dumps 2676 à 2681** (`infra/3v_18`) et relecture : exactement 6 points, un par dump (`SampleCount` 1, minimum égal au maximum).

| Dump | Fraîcheur |
|---|---|
| 2676 | 219 936,5 s (61,09 h) |
| 2677 | 220 066,3 s (61,13 h) |
| 2678 | 134 240,8 s (37,29 h) |
| 2679 | 134 368,7 s (37,32 h) |
| 2680 | 54 737,0 s (15,20 h) |
| 2681 | 54 887,4 s (15,25 h) |

`EcoutesChargees` des dumps 2676 à 2681 : somme 14 440 138 sur 6 publications. Constat visuel du tableau fait par Loïc. **Critère atteint** : la métrique est publiée, une fois par dump, et visible sur le tableau de bord. Fraîcheurs élevées : chargement de rattrapage ; le régime quotidien est à lire sur les points des chargements planifiés suivants.
