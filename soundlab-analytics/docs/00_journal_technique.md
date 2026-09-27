# SoundLab Analytics — Journal technique de mise en œuvre

**Projet de certification Bloc 6 — Big Data**
Loïc Rabetsanta — Master Architecte en Intelligence Artificielle, Jedha / Fonderie de l'Image
Compte AWS `589276558852` — Région `eu-north-1` (Stockholm)
Version 2.0

---

## 1. Contexte

SoundLab Analytics est une société fictive lyonnaise de *B2B analytics* pour labels indépendants. Le produit à construire prédit le succès commercial d'une chanson : un modèle Random Forest binaire classe une piste comme *hit* si elle appartient au quartile supérieur des écoutes.

Ce document couvre la mise en place de l'infrastructure Big Data, les pipelines d'ingestion et la démarche de conformité. Il consigne les décisions d'architecture, leur justification, les écarts au plan initial, et l'intégralité des incidents rencontrés avec leur résolution.

### Données sources

| Fichier | Volume | Contenu |
|---|---|---|
| `Music Info.csv` | 50 683 lignes × 21 colonnes | Métadonnées et 13 caractéristiques audio Spotify |
| `User Listening History.csv` | 9 711 301 lignes | Triplets `user_id` / `track_id` / `playcount` |

Source : jeu de données Kaggle `undefinenull/million-song-dataset-spotify-lastfm`, licence CC BY-NC 4.0.

### Objectifs de performance du modèle

AUC-ROC > 0,80 et Recall > 0,75. La variante retenue (13 caractéristiques audio + `unique_listeners`) atteint 0,9921 d'AUC-ROC en prototypage.

---

## 2. Architecture retenue

```
                    ┌──────────────────────────────────────┐
   Kaggle ────────► │  S3 raw        (CSV, chiffré KMS)    │
                    └───────────────┬──────────────────────┘
                                    │
                          EMR Serverless (PySpark, ARM64)
                                    │
                    ┌───────────────▼──────────────────────┐
                    │  S3 curated    (Parquet + Snappy)    │
                    │  catalogue Glue → Athena             │
                    └───────────────┬──────────────────────┘
                                    │
                         Redshift Serverless (entrepôt)
                                    │
                    ┌───────────────▼──────────────────────┐
                    │  S3 models     (MLflow, RF sérialisé)│
                    └──────────────────────────────────────┘

   Transverse :  CloudTrail (audit)  ·  CloudWatch (observabilité)
                 Secrets Manager (sel de pseudonymisation)
                 KMS (chiffrement au repos)  ·  AWS Budgets (garde-fou coût)
```

### 2.1 Arbitrages technologiques

| Besoin | Choix retenu | Écarté | Justification |
|---|---|---|---|
| Calcul distribué | **EMR Serverless** | Cluster EMR sur EC2 | Aucun coût à l'arrêt ; un seul rôle IAM contre trois ; pas de VPC à router |
| Architecture processeur | **ARM64 (Graviton)** | x86 | Environ 20 % moins cher à performance équivalente, sans adaptation de code |
| Capacité pré-initialisée | **Aucune** | Workers préchauffés | Facturation strictement à l'usage. Contrepartie assumée : 60 à 120 s de démarrage à froid, sans importance en batch |
| Entrepôt | **Redshift Serverless** | Redshift provisionné RA3 | Crédit d'essai de 300 $ sur 90 jours ; facturation à la seconde ; pause automatique |
| Orchestration | **Airflow en Docker local** | MWAA | Le plus petit environnement MWAA coûte environ 350 €/mois, facturé même à vide |
| Suivi ML | **MLflow local**, artefacts sur S3 | SageMaker | Coût nul, artefacts néanmoins traçables dans S3 |
| Chiffrement au repos | **CMK KMS + S3 Bucket Keys** | SSE-S3 seul | Politique de clé auditable, rotation annuelle, trace CloudTrail de chaque déchiffrement. Coût ≈ 1 $/mois |
| Format de stockage | **Parquet + Snappy** | CSV, Parquet + gzip | Colonnaire et compressé ; Snappy est *splittable*, contrairement à gzip |
| Catalogue | **Déclaration explicite via l'API Glue** | Crawler Glue, metastore Hive | Voir § 8, écart n° 6 et § 9.6 |

### 2.2 Choix de la région

`eu-north-1` (Stockholm) a été retenue plutôt que `eu-west-3` (Paris), malgré la domiciliation lyonnaise de l'entreprise fictive :

1. **Coût** — Stockholm figure parmi les régions AWS les moins chères d'Europe, de l'ordre de 10 à 20 % sous Paris sur EC2 et S3.
2. **Empreinte carbone** — région alimentée par de l'hydroélectricité et de l'éolien.
3. **Conformité** — la Suède appartient à l'UE et à l'EEE : aucune donnée ne sort de l'Espace économique européen.

La latence n'est pas un critère : les traitements sont des batchs analytiques nocturnes. Disponibilité des sept services requis vérifiée avant tout déploiement via les paramètres publics SSM.

---

## 3. Étape 0 — Sécurisation du compte

Un compte AWS neuf a été ouvert après l'abandon du précédent (§ 9.0).

| Action | Détail |
|---|---|
| MFA sur le compte root | Activé ; aucune clé d'accès root n'existe |
| Utilisateur IAM | `loic-admin`, `AdministratorAccess`, MFA activé |
| Accès IAM à la facturation | Activé depuis le compte root (prérequis à l'API Budgets) |
| CLI | Région figée à `eu-north-1`, sortie JSON |
| Budget | `soundlab-monthly-40` — alertes à 50 %, 80 % du réalisé, 100 % du prévisionnel |
| Anti-fuite de secrets | `gitleaks` en *pre-commit hook* |

### Authentification sans clé permanente

L'authentification repose sur `aws login` (AWS CLI ≥ 2.32) : flux OAuth 2.0 avec PKCE via le navigateur, identifiants **temporaires** renouvelés toutes les 15 minutes, valides 12 heures au maximum.

**Aucune clé d'accès longue durée ne réside sur le poste de travail.** C'est la réponse structurelle à l'incident qui a coûté le premier compte : il n'y a plus de secret persistant à exfiltrer.

---

## 4. Étape 1 — Infrastructure socle

Provisionnée par `infra/01_bootstrap.sh`, script **idempotent** qui refuse de démarrer si la région effective diffère de `eu-north-1` ou si une variable d'environnement l'écrase.

### 4.1 Stockage

| Bucket | Rôle | Chiffrement |
|---|---|---|
| `soundlab-raw-558852` | Données brutes Kaggle, immuables | SSE-KMS (CMK) |
| `soundlab-curated-558852` | Parquet produit par PySpark | SSE-KMS (CMK) |
| `soundlab-models-558852` | Artefacts MLflow, modèle sérialisé | SSE-KMS (CMK) |
| `soundlab-scripts-558852` | Jobs PySpark soumis à EMR | SSE-KMS (CMK) |
| `soundlab-logs-558852` | Journaux EMR, CloudTrail, rapports | SSE-S3 (AES256) |

Configuration commune : blocage public intégral, versioning, S3 Bucket Keys (réduit d'environ 99 % les appels KMS), politique refusant tout appel en HTTP clair, cycle de vie.

### 4.2 Chiffrement

Clé gérée par le client `alias/soundlab`, rotation annuelle automatique. Le choix d'une CMK apporte trois éléments requis par une démarche d'audit : politique de clé opposable, rotation maîtrisée, trace CloudTrail de **chaque déchiffrement**.

### 4.3 Identités et accès

Rôle `SoundLabEMRServerlessExecutionRole`, principal de confiance `emr-serverless.amazonaws.com`, deux politiques en ligne au **moindre privilège** : lecture sur `raw`/`scripts`/`curated`, écriture sur `curated`/`models`/`logs`, opérations KMS bornées à la seule clé du projet, `secretsmanager:GetSecretValue` borné au seul secret de pseudonymisation, journalisation limitée à `/aws/emr-serverless/*`, catalogue Glue limité aux bases du projet.

Aucun `s3:*` sur `*`, aucune politique administrative sur un rôle de calcul.

### 4.4 Audit et secrets

- **CloudTrail** `soundlab-trail` : multirégion, validation d'intégrité des fichiers journaux activée.
- **Secrets Manager** `soundlab/pseudonymisation-salt` : sel de 32 octets généré par `openssl rand -hex 32`, transmis directement à l'API, jamais écrit sur disque, chiffré par la CMK.

### 4.5 Durées de conservation — appliquées

Script `infra/03_retention.sh` (action A1 de l'AIPD) :

| Bucket | Expiration | Purge des versions non courantes | Effacement effectif |
|---|---|---|---|
| `raw` | 365 j | 30 j | 395 j |
| `curated` | 730 j | 30 j | 760 j |
| `logs` | 365 j | 30 j | 395 j |
| `models`, `scripts` | aucune | 30 j | — |

**Subtilité déterminante** : sur un bucket versionné, `Expiration` ne supprime pas l'objet — elle pose un marqueur de suppression et bascule la version courante en version non courante. La donnée reste intégralement récupérable, et demeure une donnée personnelle au sens du RGPD. Une règle dépourvue de `NoncurrentVersionExpiration` produit une **conformité de façade** et une conservation en réalité illimitée. Les deux règles sont posées conjointement.

---

## 5. Étape 2 — Chargement des données et moteur de calcul

### 5.1 Transfert

Réalisé depuis **AWS CloudShell**, pour deux raisons : identifiants temporaires injectés par AWS (aucune clé à manipuler), et transfert sur le réseau AWS plutôt que sur une connexion domestique.

CloudShell n'offrant que 1 Gio de stockage persistant, le transfert s'est fait en flux : `unzip -p | tee >(sha256sum) | aws s3 cp -`. Le CSV décompressé ne touche jamais le disque.

### 5.2 Empreintes d'intégrité — lignage

| Objet S3 | SHA-256 | Taille |
|---|---|---|
| `msd/music_info.csv` | `d930430f811ba3c77f217b3f456f2b6271c238b828d6d9ad76e889b5d725f187` | 14,3 Mio |
| `msd/user_listening_history.csv` | `6220d88d533d99d57888dea6e9408b168c021d6cfc1ea307a3ba042cd000df31` | 602 515 573 o |

Chiffrement au repos vérifié : `ServerSideEncryption: aws:kms`.

### 5.3 Moteur de calcul

Application EMR Serverless `soundlab-spark`, identifiant `00g8l9brbs9e3f1d` : `emr-7.14.0` (Spark 3.5.8), ARM64, capacité maximale 32 vCPU, arrêt automatique après 5 minutes d'inactivité, **aucune capacité pré-initialisée**.

---

## 6. Tâche 7 — Ingestion des métadonnées

`jobs/07_ingest_music_info.py` — `raw/msd/music_info.csv` → `curated/music_info/` (Parquet), table Glue `soundlab_curated.music_info`.

### 6.1 Décisions de conception

**Schéma déclaré, jamais `inferSchema`.** L'inférence oblige Spark à lire tout le fichier une première fois pour deviner les types, ce qui double le coût. Elle est surtout instable : une seule ligne malformée fait basculer une colonne entière. Un schéma explicite est un contrat.

**Lignes rejetées archivées, jamais jetées.** Mode `PERMISSIVE` avec `_corrupt_record`. Un pipeline qui perd des données en silence n'est pas auditable.

**Aucune imputation à l'ingestion.** La couche `curated` reste fidèle à la source ; l'imputation appartient au feature engineering.

**`coalesce(2)`, pas de partitionnement.** 50 000 lignes ne justifient pas un découpage : cela produirait des centaines de fichiers de quelques kilo-octets (*small files problem*).

### 6.2 Résultats

Exécution en **36 s**. Sortie : 2 fichiers Parquet Snappy (3,3 + 3,6 Mio), soit 52 % de moins que le CSV source malgré trois colonnes dérivées supplémentaires.

| Métrique | Valeur |
|---|---|
| Lignes lues | 50 683 |
| Lignes corrompues | 0 |
| `track_id` distincts | 50 683 (0 doublon) |
| Valeurs hors bornes Spotify | aucune |
| Lignes écrites | 50 683 |

### 6.3 Constat qualité majeur

**`genre` est nul à 55,9 %** — 28 335 pistes sur 50 683.

Cela explique rétrospectivement pourquoi la variante V3 du modèle plafonnait à **AUC-ROC 0,6579** contre **0,9921** pour V1 : plus d'une observation sur deux n'apportait aucune information sur cette variable. Le choix de V1 se justifie désormais par une mesure.

Autres taux : `tags` 2,2 %, `year` 0,016 %. Les 13 caractéristiques audio sont **complètes à 100 %** et toutes dans les bornes de l'API Spotify.

---

## 7. Tâche 8 — Ingestion de l'historique d'écoute et pseudonymisation

`jobs/08_ingest_listening_history.py` — 9,7 M lignes, pseudonymisation SHA-256 salée.

### 7.1 Ce qui s'inverse par rapport à la tâche 7

Le renversement de trois choix est en soi un argument d'architecture : on ne dimensionne pas pour la donnée qu'on a, mais pour celle qu'on traite.

| Décision | Tâche 7 (50 683 lignes) | Tâche 8 (9,7 M lignes) |
|---|---|---|
| Nombre de fichiers | `coalesce(2)` | `repartition(12)` |
| Contrôles qualité | 17 actions Spark distinctes | **une seule expression `agg()`** |
| Pseudonymisation | sans objet | SHA-256 salé, sel dans Secrets Manager |
| Validation du schéma | par position | **par nom de colonne** |

**Le repli des contrôles en une passe est l'optimisation centrale.** Chaque `count()` déclenche un parcours complet ; dix-sept parcours sont indolores sur 50 000 lignes en cache, ruineux sur 9,7 millions. Résultat mesuré : **190 fois plus de données traitées en un temps comparable** (34 s contre 36 s).

**La validation par nom plutôt que par position** évite un scénario silencieux : avec un schéma positionnel, une inversion de `user_id` et `track_id` dans la source produirait des données fausses sans lever d'erreur. Le job vérifie l'ensemble des colonnes attendues et refuse de démarrer sinon.

**Pourquoi `repartition` et non `partitionBy`.** Le partitionnement Hive en répertoires n'accélère que les requêtes filtrant sur la clé de partition. L'usage aval est une jointure sur `track_id` qui lit toute la table : aucun élagage possible. La contrainte réelle est à l'**écriture** — une partition Spark est écrite par une tâche, et 12 correspond au parallélisme disponible. À la lecture, Parquet est *splittable* : Spark découpe un même fichier aux frontières des row groups.

### 7.2 Pseudonymisation

`user_id_hash = SHA-256(sel || user_id)`, tronqué à 128 bits.

**Pourquoi saler.** Un identifiant utilisateur est court et tiré d'un espace restreint : un SHA-256 nu se casse par table arc-en-ciel en quelques minutes. Sans sel, on ne peut pas parler de pseudonymisation au sens du RGPD.

**Le hachage est déterministe, et c'est le point.** Le même utilisateur reçoit toujours le même jeton, ce qui préserve le calcul de `unique_listeners` par piste. Compromis exact de la pseudonymisation : on perd l'identité, on garde la structure.

**Contrôle anti-collision bloquant.** Le nombre de jetons distincts doit égaler le nombre d'utilisateurs distincts de la source. Une différence signifierait deux personnes fusionnées en une seule dans toutes les analyses en aval.

### 7.3 Résultats

| Métrique | Valeur |
|---|---|
| Lignes lues | 9 711 301 |
| Utilisateurs distincts | 962 037 |
| Pistes distinctes | **30 459** |
| `playcount` min / max / moyenne | 1 / 2 948 / 2,631 |
| Jetons distincts | 962 037 |
| **Collisions** | **0** |
| Durée | 34 s puis 38 s (après optimisation) |

### 7.4 Optimisation du stockage — troncature du condensat

La première exécution produisait **634 Mio de Parquet pour 575 Mio de CSV source** : la sortie compressée était plus lourde que le texte brut.

Cause : `user_id_hash` sur 64 caractères hexadécimaux, **incompressible par construction**. Un condensat est indistinguable du hasard : Snappy, qui cherche des motifs répétés, n'a pas de prise, et le dictionnaire Parquet est inutile face à 962 037 valeurs distinctes.

| | Avant (256 bits) | Après (128 bits) |
|---|---|---|
| Taille totale | 634 Mio | **317 Mio** |
| Octets par ligne | 68,5 | **34,2** |
| Collisions | 0 | 0 |

Le jeton pseudonyme représente **93 % du volume stocké** (32 des 34,2 octets par ligne). Les trois autres colonnes tiennent dans 2,2 octets grâce à l'encodage par dictionnaire et par plages.

**Distinction à tenir** : c'est une décision de *stockage*, pas de *sécurité*. La résistance à la ré-identification repose entièrement sur le secret du sel, jamais sur la longueur du condensat. Probabilité de collision théorique sur 128 bits pour 962 037 utilisateurs : environ **1,4 × 10⁻²⁷**.

**Enseignement** : le format colonnaire ne compresse que ce qui est compressible. Sur des colonnes à faible cardinalité, Parquet fait des miracles ; sur un condensat cryptographique, la seule variable d'ajustement est la longueur.

---

## 8. Tâche 9 — Tests de qualité et analyse d'impact

### 8.1 Suite de tests

`jobs/09_tests_qualite.py` — treize contrôles sur la couche `curated`, avec **code de sortie non nul** en cas d'échec bloquant. C'est ce qui en fait une *porte de qualité* exploitable par un orchestrateur : branchée dans Airflow, elle empêchera le feature engineering de tourner sur des données invalides.

Le job est séparé des pipelines d'ingestion à dessein : un pipeline qui s'auto-évalue ne peut détecter ni une dérive entre deux tables, ni une régression introduite par une exécution ultérieure.

**Chaque seuil est justifié dans le code.** Le seuil sur `genre` est à 60 %, juste au-dessus des 55,9 % mesurés : il n'alerte pas sur l'état connu mais signale une dégradation. Celui sur les caractéristiques audio est à zéro, parce que ce sont les variables du modèle. Un contrôle dont personne ne peut justifier le seuil finit désactivé au premier faux positif.

**Résultat : 13 contrôles sur 13 réussis, 0 échec bloquant, 0 avertissement.**

| Contrôle | Résultat |
|---|---|
| Volumétrie `music_info` | 50 683 lignes |
| Unicité de `track_id` | 0 doublon, 0 nul |
| Complétude des 13 caractéristiques audio | 0 % de nuls |
| Bornes de l'API Spotify | conforme |
| Nuls sur `genre` / `year` | 55,906 % / 0,016 % |
| **Absence d'identifiant direct en curated** | **conforme** |
| Volumétrie `listening_history` | 9 711 301 lignes |
| Format du jeton | 32 caractères hexadécimaux, homogène |
| Positivité de `playcount` | min 1, max 2 948 |
| Unicité de `(user_id_hash, track_id)` | 0 doublon |
| **Intégrité référentielle** | **0 orphelin sur 30 459 pistes** |
| Couverture du catalogue | 60,1 % |

### 8.2 Biais de sélection

**30 459 pistes ont un historique d'écoute, sur les 50 683 du catalogue — soit 60,1 %.**

La jointure des tâches 10-11 éliminera environ **20 224 pistes**. C'est exactement le nombre de lignes de la table finale obtenue en prototypage Colab (30 459), ce qui **valide le pipeline de bout en bout**.

C'est aussi un biais de sélection à énoncer explicitement : le modèle ne prédit pas « cette chanson sera-t-elle un succès » mais « parmi les chansons déjà écoutées au moins une fois, laquelle atteindra le quartile supérieur ». La nuance change la portée commerciale de l'outil — il ne peut pas évaluer un titre inédit sans aucun signal d'écoute.

### 8.3 Analyse d'impact (AIPD)

Document `docs/01_dpia_analyse_impact.md`, rédigé selon la méthode CNIL. Trois arguments structurels, chacun appuyé sur une mesure :

**La finalité ne porte pas sur les personnes.** Le modèle produit un score par titre. Cela exclut l'article 22 du RGPD et fait tomber la gravité de deux des trois événements redoutés à « négligeable ».

**La ré-identification est structurellement impossible pour le responsable de traitement.** Les identifiants du Taste Profile sont déjà des condensats à la source, la table de correspondance n'a jamais été publiée, et SoundLab applique par-dessus une seconde pseudonymisation. Ni le sel d'origine, ni la correspondance, ni aucune donnée directement identifiante ne se trouvent dans son système.

**La nécessité est démontrée, pas postulée.** La mise en balance de l'article 6.1.f s'appuie sur la variante V2 : sans `unique_listeners`, l'AUC-ROC tombe de 0,9921 à 0,5959, soit le niveau du hasard.

L'AIPD décrit également un mécanisme d'exercice des droits au titre de **l'article 11.2** : la personne fournit son identifiant source, le DPO seul recalcule le jeton avec le sel, la requête est exécutée via Athena, la ligne extraite ou supprimée, l'opération journalisée. Techniquement réalisable en l'état, le hachage étant déterministe et Parquet autorisant la réécriture sélective.

Cinq risques résiduels sont consignés, dont la singularisation : l'auditeur le plus actif compte **784 titres** contre une moyenne de 10, profil suffisamment distinctif pour permettre un recoupement avec une source externe. C'est le mode de ré-identification que la pseudonymisation ne protège pas.

---

## 9. Écarts au plan initial

Le plan initial consigné dans Notion a été révisé sur huit points. Aucun n'est un renoncement : chacun découle soit du changement de source de données, soit d'une mesure effectuée en cours de route.

| N° | Prévu | Réalisé | Raison |
|---|---|---|---|
| **1** | Fichiers HDF5 lus avec `h5py` | CSV Kaggle | Le dataset retenu fournit du CSV. Le point d'attention sur la lecture HDF5 en UDF Python devient sans objet |
| **2** | 54 caractéristiques audio | 21 colonnes dont 13 features Spotify | Structure réelle du dataset |
| **3** | Label `song_hotttnesss` | `is_hit` dérivé de `total_plays` | Colonne absente du dataset retenu |
| **4** | Parquet partitionné par lettre de `song_id` | `coalesce(2)` puis `repartition(12)`, **sans `partitionBy`** | Le partitionnement Hive n'accélère que les requêtes filtrant sur la clé. L'usage aval est une jointure lisant toute la table : aucun élagage possible. Le *bucketing* aiderait, mais exige un metastore Hive — abandonné (§ 9.6) |
| **5** | UDF PySpark pour le hachage | `F.sha2()` natif | Une UDF Python force une sérialisation ligne par ligne entre JVM et Python ; la fonction native s'exécute dans la JVM, sans coût de conversion |
| **6** | **Crawler AWS Glue** | **Déclaration explicite via l'API Glue** | Voir ci-dessous |
| **7** | Orchestration MWAA | Airflow en Docker local | ≈ 350 €/mois pour le plus petit environnement, facturé même à vide. MWAA figure dans l'architecture *cible*, l'implémentation de démonstration est locale |
| **8** | Métriques de qualité dans CloudWatch | Rapports JSON horodatés dans S3 + logs CloudWatch | Le JSON est versionnable, comparable entre exécutions et directement citable dans le rapport de certification |

### Écart n° 6 — Pourquoi aucun crawler Glue

Le plan initial prévoyait un crawler configuré sur le bucket pour cataloguer automatiquement les tables Parquet. Ce choix a été écarté, et le remplacement par une déclaration explicite via `glue:create_table` n'est pas un contournement : c'est une décision de conception.

**Un crawler devine, une déclaration engage.** Un crawler échantillonne les fichiers, infère un schéma, puis écrit son inférence dans le catalogue. Une déclaration explicite transmet le schéma décidé, avec ses types choisis. Le premier s'adapte silencieusement à ce qu'il trouve ; le second constitue un contrat qui rompt bruyamment si la donnée dévie. C'est exactement le raisonnement qui a fait écarter `inferSchema` à la tâche 7 — appliqué au catalogue plutôt qu'au fichier.

**Un crawler écraserait des métadonnées porteuses de sens.** Les tables déclarées portent des paramètres de conformité, notamment `donnees_personnelles: pseudonymisees` sur `listening_history`. Un crawler ne les régénérerait pas : il les remplacerait par ses propres inférences. Installer un crawler serait donc une **régression** de la traçabilité, pas une amélioration.

**Un crawler introduit une dérive non maîtrisée.** Il s'exécute selon un calendrier et modifie le catalogue sans revue. Un schéma qui change entre deux exécutions sans qu'aucun humain ne l'ait décidé est précisément ce qu'un pipeline reproductible cherche à éviter.

**Ce que l'on perd, et pourquoi cela ne pèse pas ici.** Un crawler découvre automatiquement de nouvelles partitions et de nouvelles tables. C'est utile sur un lac de données alimenté par des sources hétérogènes et non maîtrisées. Ici, les deux tables sont produites par des jobs que nous écrivons, avec un schéma que nous décidons : il n'y a rien à découvrir.

Le coût n'est pas l'argument — un crawler coûte quelques centimes par exécution. L'argument est la maîtrise du schéma.

---

## 10. Journal des incidents

Chaque incident est consigné selon la même structure : symptôme, cause réelle, résolution, enseignement. Plusieurs de ces enseignements ont plus de valeur qu'un pipeline qui aurait fonctionné du premier coup.

### 10.0 — Abandon du premier compte AWS

**Symptôme.** Trois défaillances cumulées : clés d'accès exposées puis révoquées par AWS ; permissions IAM insuffisantes pour créer un cluster EMR ; ressources dispersées entre deux régions.

**Résolution.** Compte neuf avec, dès le départ : MFA sur root, aucune clé root, `gitleaks` en pre-commit, budget d'alerte, puis `aws login` pour supprimer toute clé permanente. EMR Serverless élimine par construction la difficulté IAM. Chaque script refuse de démarrer hors de la bonne région.

**Enseignement.** Les trois pannes n'étaient pas indépendantes : elles découlaient d'une absence de garde-fous automatiques. La parade n'est pas « faire plus attention », c'est rendre l'erreur structurellement impossible.

### 10.1 — Le budget n'a pas été créé

**Cause.** L'accès des utilisateurs IAM aux informations de facturation n'était pas activé au niveau du compte. Un utilisateur IAM, même administrateur, est refusé par l'API Budgets tant que ce réglage n'est pas basculé depuis le compte root.

**Note.** L'API Budgets est globale et ne répond qu'en `us-east-1`, même pour un projet entièrement hébergé à Stockholm.

### 10.2 — Région incohérente entre profil et environnement

**Cause.** Le profil nommé n'existait pas ; la commande lisait le profil `default`. Par ailleurs, une variable d'environnement `AWS_REGION` prime toujours sur le profil.

**Résolution.** Garde-fou en tête de chaque script :

```bash
EFF_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region)}}"
[[ "$EFF_REGION" == "eu-north-1" ]] || die "Région incohérente"
```

**Enseignement.** L'ordre de précédence des sources de configuration AWS — variable d'environnement, puis profil nommé, puis profil par défaut — est une cause classique de déploiement dans la mauvaise région. Le vérifier coûte trois lignes.

### 10.3 — Quota de vCPU du compte

**Symptôme.** `ServiceQuotaExceededException: the account has reached the service limit on the maximum vCPU it can use concurrently`.

**Cause.** Un compte neuf démarre avec un quota EMR Serverless de **16 vCPU concurrents** (`L-D05C8A75`). Le job demandait 18, et jusqu'à 26 avec l'allocation dynamique.

**Enseignement — le plus important.** *Le plafond réel d'un traitement distribué n'est pas la capacité déclarée de l'application, mais le quota du compte.* La `maximumCapacity` de l'application était fixée à 32 vCPU : elle n'a servi à rien. Rien dans la configuration ne laissait deviner l'erreur.

### 10.4 — Glue : `AccessDenied` puis `NullPointerException`

**Cause.** Le client metastore Hive vérifie puis tente de créer la base `default` **au démarrage**, indépendamment de la base réellement visée. La politique IAM ne listait que la base du projet ; après élargissement, l'initialisation du délégué échouait toujours.

**Résolution.** Création manuelle de la base `default`, puis suppression complète de la dépendance au metastore (§ 10.5).

### 10.5 — Décision : abandon du pont Hive/Glue

**Constat.** `enableHiveSupport()` + `saveAsTable()` fait transiter Spark par `AWSGlueDataCatalogHiveClientFactory`, qui introduit trois points de défaillance — vérification de `default`, création de `default`, initialisation du délégué — **avant la première ligne de données lue**.

**Résolution.** Écriture Parquet nue, puis déclaration de la table par un appel `glue:create_table` explicite portant colonnes, SerDe, format et emplacement.

**Enseignement.** *Les abstractions ont un coût de fiabilité.* `saveAsTable` était plus élégant à lire ; la déclaration explicite est plus verbeuse, entièrement déterministe, et offre un contrôle exact du schéma publié.

### 10.6 — Quarante et une minutes facturées pour une erreur de connexion

**Symptôme.** `ConnectTimeoutError` sur un endpoint S3. Durée du job : 2 445 secondes. Consommation facturée : 4,067 vCPU-heures.

**Cause.** Le client boto3 était instancié sans région explicite et visait donc l'endpoint S3 *global*, qui ne résout pas correctement pour les régions ouvertes après 2019 — dont `eu-north-1`. La stratégie de retry par défaut a ensuite retenté pendant des dizaines de minutes.

**Résolution.**

```python
CONFIG_AWS = Config(connect_timeout=5, read_timeout=30,
                    retries={"max_attempts": 3, "mode": "standard"})
s3 = boto3.client("s3", region_name=args.region, config=CONFIG_AWS)
```

**Enseignement.** *Sur une plateforme facturée à la seconde, la stratégie de retry est une décision d'architecture.* Les valeurs par défaut d'un SDK sont pensées pour des applications interactives, pas pour du calcul facturé.

### 10.7 — Corruption silencieuse d'ARN par zsh

**Symptôme.** `MalformedPolicyDocument: The policy failed legacy parsing` sur un document JSON pourtant valide.

**Cause.** Dans un heredoc zsh, `:c` et `:t` sont des **modificateurs de paramètre** (*resolve command path*, *tail*). `$VAR:catalog` et `$VAR:table` étaient donc tronqués :

```
arn:aws:glue:eu-north-1:589276558852atalog       ← ":c" consommé
arn:aws:glue:eu-north-1:589276558852able/default ← ":t" consommé
arn:aws:glue:eu-north-1:589276558852:database/…  ← intact, ":d" n'est pas un modificateur
```

La ligne `:database` ayant survécu, l'anomalie était difficile à repérer visuellement.

**Résolution.** Accolades systématiques : `${VAR}:catalog`. Et validation du JSON avant tout appel : `python3 -m json.tool`.

**Enseignement.** Le message « failed legacy parsing » désigne souvent un ARN invalide, pas un JSON malformé. Lire le fichier généré avant de le soumettre coûte une seconde et évite une demi-heure de fausse piste.

### 10.8 — Contrainte non documentée sur l'allocation dynamique

**Cause.** EMR Serverless injecte son propre défaut `spark.dynamicAllocation.initialExecutors = 3` et valide que cette valeur tienne dans `[minExecutors, maxExecutors]`. Un plafond fixé à 2 rendait ce défaut invisible invalide.

**Enseignement.** Une plateforme managée superpose ses propres défauts à ceux du moteur. Déclarer explicitement les paramètres qu'on croit maîtriser évite d'être arbitré par une valeur jamais vue.

### 10.9 — Application EMR en état `TERMINATED`

**Cause.** L'application avait été supprimée pour être recréée en version plus récente, sans que la recréation soit enchaînée.

**Piège associé.** Les noms d'application EMR Serverless ne sont pas uniques, et les applications supprimées restent listées un certain temps. Toute recherche par nom doit filtrer l'état :

```bash
aws emr-serverless list-applications \
  --query "applications[?name=='soundlab-spark' && state!='TERMINATED'].id" --output text
```

### 10.10 — Conformité de façade évitée sur les cycles de vie

**Symptôme.** Aucun — c'est le propre du problème.

**Cause.** Sur un bucket versionné, une règle `Expiration` ne supprime pas l'objet : elle pose un marqueur de suppression et bascule la version courante en version non courante. L'objet disparaît des listages, la donnée reste intégralement récupérable, et demeure une donnée personnelle au sens du RGPD.

**Résolution.** `NoncurrentVersionExpiration` posée conjointement à `Expiration` sur les cinq buckets. Effacement effectif = durée annoncée + 30 jours de purge technique, ce que l'AIPD énonce désormais explicitement.

**Enseignement.** Une mesure de conformité non vérifiée est une hypothèse. Celle-ci aurait produit un document exact et une infrastructure non conforme, sans qu'aucune alerte ne se déclenche.

### 10.11 — Incidents mineurs

| Symptôme | Cause | Résolution |
|---|---|---|
| CloudShell : `Too Many Requests` au téléversement | Limitation de débit temporaire | Publication du script dans S3 depuis le poste local — EMR lit depuis S3, pas depuis CloudShell |
| `sed` sans effet sur `submit_job.sh` | Motif préfixé d'une espace absente en début de ligne | Motif ancré sur `^`, suppression de ligne plutôt que substitution |
| `SL_SECRET_ARN` vide au lancement de la tâche 8 | Variable absente du `.soundlab.env` reconstruit à la main | Récupération par `describe-secret` et ajout au fichier |
| Sortie JSON parasite du script d'infrastructure | Réponse de l'API lifecycle non redirigée | `>/dev/null` ajouté |

### 10.12 — Un garde-fou qui se déclenche à tort et n'explique pas pourquoi

**Symptôme.** Le script de création du rôle IAM s'est interrompu sur « ARN suspecte détectée dans la politique », suivi d'une liste vide.

**Cause.** Deux fautes cumulées. Le motif de détection contenait l'alternative `pplications/[^"]*[^0-9a-z]`, qui correspondait à l'ARN parfaitement valide `.../applications/00g8l9brbs9e3f1d/jobruns/*` — le `/` de `jobruns` satisfaisant `[^0-9a-z]`. Et le second `grep`, celui chargé d'**afficher** le coupable, n'incluait pas cette alternative : d'où le message d'erreur vide.

**Correction.** Abandon de la détection par signatures au profit d'un **contrôle structurel** : une ARN canonique a six segments séparés par des deux-points et un identifiant de compte à douze chiffres. Une corruption par modificateur zsh supprime un deux-points, donc réduit le nombre de segments. Le contrôle a été testé sur les deux corruptions réelles de l'incident 10.7 et sur les treize ARN de la politique.

> **Chercher des signatures d'erreur connues suppose qu'on les connaît toutes. Vérifier une structure attendue ne suppose rien.**

---

### 10.13 — Un test négatif qui échouait pour la mauvaise raison

**Symptôme.** Le test « l'orchestrateur ne peut pas lire une donnée source » échouait — donc passait — mais avec l'erreur `NoSuchKey` et non `AccessDenied`.

**Cause.** Subtilité S3 : le service ne répond `AccessDenied` sur une clé inexistante que si l'appelant n'a pas `s3:ListBucket` sur le compartiment. S'il l'a, S3 révèle `NoSuchKey`. La politique accordait `ListBucket` sur les cinq compartiments, `raw` compris. Le rôle **ne pouvait effectivement pas** lire les objets — la permission était correcte — mais le test ne le démontrait pas.

**Correction, en deux temps.** D'abord resserrer la politique : l'orchestrateur n'a aucune raison d'énumérer le compartiment des données brutes, puisque le job EMR le lit avec son propre rôle d'exécution. Ensuite durcir la vérification : un test négatif exige désormais que la sortie contienne `AccessDenied` ou équivalent, et signale `INDÉCIS` sinon.

> **Un test négatif qui échoue pour la mauvaise raison est indiscernable d'un test qui réussit.** C'est le même piège que la corrélation de Pearson sur la fuite de cible : le bon résultat obtenu par le mauvais instrument.

---

### 10.14 — Vérifier une politique juste après l'avoir modifiée, c'est mesurer l'ancienne

**Symptôme.** Après resserrement de la politique, le test « énumérer le compartiment des données brutes » a **réussi** alors qu'il devait être refusé. Le simulateur IAM, interrogé juste après, répondait pourtant `implicitDeny`.

**Cause.** IAM est à cohérence différée. L'exécution précédente avait patienté dix secondes avant de parvenir à assumer le rôle, offrant gratuitement le délai de propagation. Celle-ci a réussi du premier coup : les tests se sont exécutés contre la version précédente de la politique.

**Correction.** Plutôt qu'un délai arbitraire, le script **attend la condition exacte** : il interroge `simulate-principal-policy` jusqu'à ce que la décision sur `raw` devienne un refus, c'est-à-dire jusqu'à ce que la politique évaluée soit celle qui vient d'être publiée. Il abandonne au bout de soixante secondes plutôt que de tester à l'aveugle.

**Deux niveaux de vérification désormais coexistent, et c'est délibéré.** Le simulateur répond à « qu'est-ce que ce rôle a le droit de faire » — sans dépendre de l'existence des objets ni du comportement de S3. Les tests réels répondent à « que se passe-t-il quand on essaie » — et captent ce que le simulateur ignore : politiques de compartiment, conditions de chiffrement, SCP d'organisation. Une politique validée seulement par simulation peut échouer en production ; une politique validée seulement par tests réels peut passer pour de mauvaises raisons.

> **Trois incidents consécutifs, et aucun n'était une erreur de permission.** C'étaient à chaque fois des erreurs de *méthode de vérification* : un motif de détection trop large, un test qui ne vérifiait pas son motif d'échec, une mesure prise avant que l'objet mesuré n'existe. Les garde-fous ont tenu : ils ont refusé trois fois de valider quelque chose de non démontré.

### 10.15 — Une alarme qui annonce « tout va bien » parce qu'elle ne reçoit rien

**Symptôme.** Une alarme CloudWatch sur les requêtes Redshift en échec affichait `OK`. Le courriel de notification envoyé par AWS disait textuellement :

> *has entered the **OK** state, because "no datapoints were received for 1 period and 1 missing datapoint was treated as [NonBreaching]".*

**Cause.** La métrique `UserQueriesFailed` n'apparaît dans `list-metrics` qu'après avoir été émise au moins une fois. Pour créer l'alarme malgré tout, le script empruntait son jeu de dimensions à une métrique voisine du même service, `QueriesSucceeded`. Or les deux n'ont pas le même schéma : `UserQueriesFailed` n'est publiée qu'avec `QueryType`, sans `Workgroup`, tandis que `QueriesSucceeded` l'est avec `Workgroup + QueryType`. L'alarme surveillait donc une combinaison que personne n'émet — et le paramètre `treat-missing-data: notBreaching` transformait ce silence total en `OK`.

**Découverte annexe.** `UserQueriesFailed / QueryType=SELECT` existait, et pour une raison instructive : c'était **notre propre test négatif** de permissions, le `SELECT` sur les tables témoins délibérément refusé. Les jeux de dimensions publiés par CloudWatch reflètent ce qui a **réellement été émis**, pas un schéma théorique.

**Correction en trois temps.** L'emprunt de dimensions est supprimé — il reposait sur une hypothèse d'homogénéité fausse. L'alarme non validable n'est plus créée, et celle qui existait est détruite : trois alarmes vérifiées valent mieux que quatre dont une ment. Et surtout, un audit final lit désormais le **motif** de l'état plutôt que l'état, en distinguant trois verdicts :

| Verdict | Signification |
| --- | --- |
| `OK (mesure active)` | La métrique produit des points en ce moment |
| `OK (au repos — jeu de dimensions valide)` | Aucune donnée maintenant, mais des points sur sept jours : la métrique est événementielle et le service est inactif |
| `MUETTE — aucune donnée sur 7 jours` | Rien n'a jamais été mesuré alors que l'activité est avérée : configuration fautive, le script sort en erreur |

Cette distinction m'avait échappé à la première rédaction : je classais les alarmes en « incident » et « capacité », alors que la vraie séparation est entre **métriques événementielles** — émises pendant une activité — et **métriques continues**. Sur un service serverless au repos, une métrique de capacité muette est normale ; une métrique de quota muette ne l'est pas.

> **Un dispositif de surveillance qui ne distingue pas « rien à signaler » de « je ne mesure rien » ne surveille pas.**

La preuve du phénomène n'est pas archivée sous forme de courriel, mais **reproductible par commande** :

```bash
aws cloudwatch describe-alarms \
  --query 'MetricAlarms[].[AlarmName,StateValue,StateReason]' --output table
```

Le champ `StateReason` fait dire à chaque alarme, par le service lui-même, *pourquoi* elle est dans son état. Une alarme sans données y annonce qu'elle traite l'absence de point comme un non-dépassement — c'est-à-dire qu'elle affiche `OK` sans rien observer. C'est exactement ce que le courriel d'AWS cité plus haut énonçait, à ceci près qu'une commande se redemande quand un courriel se perd.

**Choix assumé :** conserver une pièce jointe aurait été plus spectaculaire, s'appuyer sur une commande est plus sûr. Une preuve dont la conservation dépend d'une boîte de courriel n'est pas une preuve d'ingénierie.

---

### 10.16 — Un résultat juste, et inutilisable

**Symptôme.** Le référentiel d'artistes s'est exporté sans la moindre erreur : sept instructions réussies, un fichier Parquet écrit, tous les contrôles au vert. Mais dans l'aperçu des quinze artistes les plus écoutés, **cinq avaient un genre à `NULL`** — dont Metallica, Bon Iver et The Black Keys.

**Cause.** Ma définition était « le genre du titre le plus écouté de l'artiste ». Elle est défendable, et elle était correctement implémentée. Seulement la source ne renseigne le genre que pour 44,1 % des pistes, et lorsque le titre de tête fait partie des 55,9 % muets, la définition renvoie `NULL` alors que l'information existe ailleurs dans le catalogue de l'artiste.

**Mesure avant correction.** Deux hypothèses restaient ouvertes : ou bien le genre manque piste par piste au hasard — et il y a de l'information à récupérer — ou bien il manque par artiste entier, et `NULL` est la seule réponse correcte. Une requête a tranché : sur les 2 937 artistes sans genre, **1 959 n'ont réellement aucun titre renseigné**, et 978 en ont au moins un. Ce sont ces 978, soit 15,8 % du référentiel, que ma définition jetait.

**Correction.** `FIRST_VALUE(genre IGNORE NULLS)` retient le titre le plus écouté *qui déclare un genre*. Avec, au passage, un `NULLIF(TRIM(genre), '')` : la source mélange absence et chaîne vide, et sans cette normalisation `IGNORE NULLS` aurait retenu une chaîne vide comme un genre valide — le défaut aurait été masqué au lieu d'être corrigé. Après remplacement de la vue, `sans_genre` tombe à 1 959, la valeur prédite.

**Ce que la correction coûte, et comment on le dit.** Pour un artiste dont les titres les plus écoutés sont tous muets, le genre retenu peut venir d'un titre marginal : The Black Keys sort `rock` sur **8 titres de 53**. La vue expose donc `nb_titres_avec_genre`. Une valeur assise sur 8 titres sur 53 ne se lit pas comme celle de Coldplay, assise sur 28 sur 43 — et c'est au lecteur de la pondérer, pas à moi de le décider à sa place en silence.

**Contrôle de non-régression involontaire.** Les totaux de la version 2 sont strictement identiques à ceux de la version 1 — 6 207 artistes, 30 459 titres, 25 549 912 écoutes, 7 617 hits. Je n'avais pas conçu cela comme un test, mais c'en est un : la modification n'a touché que la colonne genre.

> **Les quatre incidents précédents portaient sur des vérifications qui validaient le mauvais objet. Celui-ci porte sur une vérification qui n'existait pas.** Aucun contrôle automatique n'aurait signalé ce défaut : le calcul était exact, la volumétrie cohérente, l'export conforme. Il n'a été vu que parce que le fichier SQL affiche quinze lignes de résultat avant d'exporter. **Un livrable de données doit se donner à lire, pas seulement à compter** — sans quoi on livre des colonnes vides avec un bilan tout vert.

---

### 10.17 — Une vérification que son propre énoncé suffit à satisfaire

**Symptôme.** Un fichier de preuve devait contenir un courriel d'AWS. Le contrôle posé était `grep -ci "no datapoints were received"`, la phrase que le courriel contient. Il a renvoyé `1`. Le fichier ne contenait pourtant pas le courriel : il contenait **le bloc de commandes lui-même**, collé par erreur — et ce bloc inclut la ligne `grep -ci "no datapoints were received"`.

**Cause.** La chaîne recherchée figurait dans l'instruction qui la recherche. Toute vérification cherchant une chaîne écrite dans sa propre consigne peut être satisfaite par cette consigne.

**Mécanisme annexe.** Le presse-papier servait à deux usages concurrents : transporter le courriel, et transporter les commandes à exécuter. Chaque copie écrasait la précédente. La méthode était fautive avant même le contrôle — **elle utilisait la même ressource pour deux choses à la fois.**

**Correction.** Les contrôles discriminants sont devenus **négatifs** : le fichier ne doit contenir ni `pbpaste`, ni le nom du dossier local. Un courriel d'AWS ne peut pas contenir ces chaînes ; un collage fautif, si. Une chaîne attendue peut arriver par accident ; une chaîne interdite, non.

**Épilogue.** La pièce a finalement été abandonnée au profit d'une preuve reproductible par commande (§ 10.15). Le meilleur remède à une preuve fragile n'était pas de mieux la vérifier, mais de ne plus en dépendre.

> **Une vérification dont l'énoncé suffit à la satisfaire ne vérifie rien.** C'est la forme la plus discrète du défaut des incidents 10.12 à 10.16 : ici, le contrôle ne se trompait pas de cible ni de moment — il se mesurait lui-même.

---

## 11. Coûts

| Poste | Coût |
|---|---|
| Clé KMS | ≈ 1,00 $/mois |
| Secrets Manager | ≈ 0,40 $/mois |
| Stockage S3 (≈ 950 Mo) | ≈ 0,03 $/mois |
| Stockage CloudTrail | ≈ 0,02 $/mois |
| EMR Serverless — 5 exécutions réussies | ≈ 0,10 $ |
| EMR Serverless — exécutions en échec | ≈ 0,25 $ (dont 4,07 vCPU-h sur l'incident § 10.6) |
| Athena | < 0,01 $ (plancher de 10 Mo par requête) |
| **Total à date** | **≈ 1,9 $** |

Budget d'alerte à 40 $/mois. Redshift Serverless bénéficiera du crédit d'essai de 300 $ valable 90 jours.

---

## 12. Inventaire des ressources

| Ressource | Identifiant |
|---|---|
| Compte AWS | `589276558852` |
| Région | `eu-north-1` |
| Utilisateur IAM | `loic-admin` |
| Buckets | `soundlab-{raw,curated,models,scripts,logs}-558852` |
| Clé KMS | `alias/soundlab` → `716b5b3a-5b92-4540-a0de-355485eac79d` |
| Rôle d'exécution | `SoundLabEMRServerlessExecutionRole` |
| Application EMR Serverless | `00g8l9brbs9e3f1d` — `emr-7.14.0`, ARM64 |
| Base Glue | `soundlab_curated` — tables `music_info`, `listening_history` |
| Secret | `soundlab/pseudonymisation-salt` |
| CloudTrail | `soundlab-trail` (multirégion) |
| Budget | `soundlab-monthly-40` |
| Quota dimensionnant | 16 vCPU concurrents (`L-D05C8A75`) |

---

## 13. Contenu du dépôt

```
soundlab-analytics/
├── .soundlab.env                       # variables d'environnement (non versionné)
├── .gitignore
├── data/
│   └── MANIFEST.sha256                 # empreintes des sources — lignage
├── docs/
│   ├── 00_journal_technique.md         # ce document
│   ├── 01_dpia_analyse_impact.md       # analyse d'impact RGPD
│   ├── 02_benchmarks_redshift.md       # mesures de performance de l'entrepôt
│   ├── 03_rapport_modelisation.md      # ablation, comparaison, SHAP
│   ├── architecture.svg                # livrable 1 — diagramme vectoriel
│   └── architecture.png                # export 2680 × 1580 pour le diaporama
├── infra/
│   ├── 01_bootstrap.sh                 # provisionnement idempotent du socle
│   ├── 02_datasets_et_emr.sh           # chargement des données + application EMR
│   ├── 03_retention.sh                 # durées de conservation (action A1)
│   ├── 04_redshift.sh                  # groupe de travail, rôle S3, plafond d'usage
│   ├── 07_droits_unload.sh             # droit d'écriture sur le préfixe d'export
│   ├── rs.sh                           # pilote SQL via la Data API
│   └── submit_job.sh                   # soumission et suivi d'un job EMR Serverless
├── jobs/
│   ├── 07_ingest_music_info.py         # ingestion des métadonnées
│   ├── 08_ingest_listening_history.py  # ingestion + pseudonymisation
│   ├── 09_tests_qualite.py             # porte de qualité, 13 contrôles
│   └── 10_feature_engineering.py       # variables, cible is_hit, agrégat d'engagement
├── ml/
│   ├── 12_entrainement_ablation.py     # entraînement et étude d'ablation
│   ├── 13_comparaison_modeles.py       # comparaison de familles de modèles
│   ├── 14_shap_evaluation.py           # interprétabilité et évaluation finale
│   └── requirements.txt
├── sql/
│   ├── 01_schema_etoile.sql            # schéma en étoile, clés de distribution et de tri
│   ├── 02_benchmarks.sql               # mesures naïf contre optimisé
│   └── 03_unload_referentiel.sql       # vue référentiel d'artistes + UNLOAD
├── orchestration/
│   ├── 00_init_airflow.sh              # Airflow 2.10 en conteneur, LocalExecutor
│   ├── 01_role_airflow.sh              # rôle et politique de l'orchestrateur, 13 vérifications
│   ├── 02_jetons.sh                    # jetons temporaires injectés dans le conteneur
│   ├── 05_droits_redshift.sh           # droits de base pour l'identité du DAG
│   ├── 06_surveillance.sh              # SNS, trois alarmes, tableau de bord CloudWatch
│   └── dags/
│       ├── 10_pipeline_soundlab.py     # DAG en losange, cinq tâches
│       └── sql/03_chargement_redshift.sql
├── rapport/
│   ├── rapport_soundlab.docx           # livrable 4 — le rapport complet
│   ├── rapport_soundlab.pdf            # version de lecture, 38 pages
│   ├── build_rapport.js                # générateur docx-js
│   ├── pages.py                        # extraction des pages de début de section
│   └── pages.json                      # numéros injectés dans le sommaire
└── slides/
    ├── build_deck.js                   # générateur pptxgenjs — le diaporama est un artefact
    ├── soutenance_soundlab.pptx        # livrable 6
    └── soutenance_soundlab.pdf
```

---

## 14. Reste à réaliser

| Tâche | Objet | Criticité |
|---|---|---|
| 10-11 | Feature engineering et normalisation PySpark, table `songs_features_labeled` | Haute — produit la table à charger dans l'entrepôt |
| 14 | **Orchestration Airflow** — DAG à cinq tâches | ✅ Livré, voir § 16 |
| 12-13 | Entraînement Random Forest, suivi MLflow, évaluation et SHAP | ✅ Livré, voir `docs/03_rapport_modelisation.md` |
| 21 | **Surveillance CloudWatch** — trois alarmes, tableau de bord, chaîne SNS vérifiée | ✅ Livré, voir § 17 |
| 9.5 (ext.) | **Restitution UNLOAD** — référentiel d'artistes exporté vers le lac | ✅ Livré, voir § 18 |
| 22 | **Rapport complet du projet** — livrable n° 4 du brief | ✅ Livré, voir § 19 |
| 15, 17-20, 23-24 | Dérive Evidently, tests d'intégration, API de score, intégration continue, compléments de diaporama, archive de rendu | Moyenne |

### Livrables attendus par le brief

Archive `loic_rabetsanta_projet6_AIA02.zip` :

| N° | Livrable | État |
|---|---|---|
| 1 | Diagramme d'architecture Big Data | ✅ `docs/architecture.svg` — vectoriel, avec export PNG 2680 × 1580 pour le diaporama |
| 2 | Scripts PySpark commentés | ✅ quatre jobs livrés, plus le DAG Airflow qui les enchaîne (`orchestration/`) |
| 3 | Configuration et scripts d'optimisation Redshift | ✅ `sql/01_schema_etoile.sql`, `sql/02_benchmarks.sql`, `sql/03_unload_referentiel.sql`, `infra/04_redshift.sh` et `infra/07_droits_unload.sh`, analyse dans `docs/02_benchmarks_redshift.md` |
| 4 | Rapport complet du projet | ✅ `rapport/rapport_soundlab.docx` — 38 pages, structuré sur les cinq critères d'évaluation du brief. Ce journal, l'AIPD, l'analyse de benchmarks et le rapport de modélisation en constituent la matière et l'accompagnent dans l'archive |
| 5 | Documentation sécurité, accès et surveillance | ✅ AIPD + § 4 de ce document |
| 6 | Diaporama de soutenance | ✅ v2 — `slides/soutenance_soundlab.pptx`, 18 diapositives avec notes d'orateur. Deux diapositives à ajouter (démo API, supervision et CI/CD) quand les tâches correspondantes seront livrées |

---

## 15. Le diaporama comme artefact reproductible

Le diaporama de soutenance n'a pas été monté à la main dans PowerPoint mais **généré par un script** (`slides/build_deck.js`, bibliothèque `pptxgenjs`). Le choix mérite d'être justifié, car il a un coût initial supérieur.

**La raison est la même que pour l'infrastructure.** Les chiffres du projet ont bougé jusqu'au dernier jour — l'AUC est passée de 0,9921 à 0,7875 après la découverte de la fuite, le volume Parquet de 634 à 317 Mio après la troncature du condensat. Chaque révision d'un chiffre dans un fichier monté à la main est une occasion d'oublier une occurrence et de présenter au jury deux valeurs contradictoires. Dans un générateur, la valeur est écrite une fois et la mise en page est recalculée.

**Le diagramme suit la même logique** : il est écrit en SVG à la main plutôt que dessiné dans un outil, donc versionnable, diffable, et modifiable par recherche-remplacement quand une métrique change.

**Contrôle qualité appliqué au rendu.** Le fichier produit est vérifié en trois passes : validation du paquet OOXML (`scripts/office/validate.py`), extraction du texte pour relecture du contenu, puis conversion en images et inspection visuelle de chaque diapositive. Cette dernière passe a révélé un débordement de texte hors de sa carte et une douzaine de défauts d'espacement qu'aucune des deux premières ne pouvait détecter — un fichier peut être structurellement valide et visuellement fautif.

---

## 16. Tâche 14 — Orchestration Airflow

### 16.1 Arbitrage : MWAA écarté

Le plan initial prévoyait Amazon MWAA. Un environnement MWAA facture un serveur web, un planificateur et un travailleur **en continu, même à vide** — de l'ordre de 350 €/mois. Sur un projet dont le coût AWS cumulé est de quelques dollars, ce seul service aurait multiplié la dépense par deux cents pour un DAG déclenché quelques fois.

Retenu : **Airflow 2.10.5 dans un conteneur Docker local**, en `LocalExecutor`. Le compose officiel d'Airflow démarre six conteneurs — Redis, un travailleur Celery, un *triggerer*, Flower — pour ce que deux services suffisent à faire ici. Le code du DAG est rigoureusement identique ; ce qui change est l'hébergement du planificateur, pas la compétence démontrée.

### 16.2 La forme du graphe

```
07_ingestion_music_info ──┐
                          ├──> 09_porte_qualite ──> 10_feature_engineering ──> 11_chargement_redshift
08_ingestion_listening ───┘
```

Ce n'est **pas une chaîne mais un losange**. Les deux ingestions lisent deux fichiers sources différents et ne dépendent pas l'une de l'autre. C'est la tâche 9 qui a besoin des deux, parce qu'elle vérifie l'intégrité référentielle entre les tables.

**Dépendance logique et contrainte de ressource sont exprimées séparément.** Le graphe autorise l'exécution parallèle de 07 et 08 ; le quota de 16 vCPU concurrents du compte l'interdit, puisque chaque job en consomme 14. La solution retenue est un **pool Airflow à un seul emplacement** — une contrainte de capacité déclarée à côté du graphe. Inventer une fausse dépendance `07 → 08` aurait produit le même comportement en mentant sur la nature du pipeline. Si le quota passait à 32 vCPU, il suffirait de porter le pool à deux emplacements : le DAG resterait inchangé.

**La porte de qualité est une propriété du graphe, pas une convention.** Le job 09 sort en code 1 si un contrôle bloquant échoue ; EMR traduit en `FAILED`, Airflow en `upstream_failed`. Les tâches aval ne s'exécutent jamais. Redshift ne peut structurellement pas être chargé avec des données non validées.

### 16.3 Choix d'implémentation : boto3 plutôt que l'opérateur natif

Le fournisseur `apache-airflow-providers-amazon` 9.2.0 est présent dans l'image, donc `EmrServerlessStartJobOperator` était disponible. Il n'a pas été retenu, pour trois raisons issues de ce projet :

**Le délai d'exécution devait être une décision visible.** L'incident 10.6 — 41 minutes facturées pour une erreur de connexion — vient d'une politique de réessai par défaut que personne n'avait choisie. `executionTimeoutMinutes=20` est une ligne de code lisible, pas un comportement hérité.

**La trace Spark devait remonter dans le journal Airflow.** C'est la raison pour laquelle le rôle d'orchestration a reçu la lecture du compartiment de journaux : en cas d'échec, la tâche affiche les soixante dernières lignes de `SPARK_DRIVER/stderr.gz` plutôt qu'un laconique « FAILED » qui obligerait à ouvrir la console AWS.

**Les abstractions ont un coût de fiabilité** — leçon déjà consignée lors de l'abandon du pont Hive/Glue (§ 10.5). Ajouter une couche dont les modes de défaillance sont inconnus pour économiser trente lignes de code irait contre ce principe.

### 16.4 Sécurité : aucune clé permanente dans le conteneur

```
clé permanente (poste, jamais partagée)
      │ sts:AssumeRole
      ▼
identifiants temporaires, 4 h maximum, droits restreints  ──►  conteneur Airflow
```

Un rôle `SoundLabAirflowRole` a été créé avec une politique limitée au strict nécessaire : soumettre des jobs sur **cette** application EMR, transmettre le rôle d'exécution **à EMR uniquement** par condition `iam:PassedToService`, lire `curated` / `models` / `scripts` / `logs`, écrire dans `scripts` seulement, et exécuter du SQL sur **ce** groupe de travail Redshift.

**L'orchestrateur n'a aucun accès au compartiment des données brutes** — ni lecture, ni énumération. Le job EMR lit `raw` avec son propre rôle d'exécution. C'est une propriété vérifiée par deux tests, pas une affirmation.

**Le sel de pseudonymisation ne transite jamais par Airflow.** Le DAG transmet l'ARN du secret comme une chaîne ; c'est le rôle d'exécution EMR qui le résout. La politique de l'orchestrateur ne contient aucune permission `secretsmanager`.

**Treize vérifications** couvrent le rôle : cinq par simulation IAM, trois autorisations réelles et cinq refus dont le motif de permission est confirmé. La démarche et ses trois incidents sont détaillés aux § 10.12 à 10.14.

**Identité Redshift.** L'API de données mappe chaque identité IAM vers un utilisateur de base distinct. Les tables appartenant à `IAM:loic-admin`, l'orchestrateur se présente sous `IAMR:SoundLabAirflowRole` et n'aurait eu aucun droit. Le nom de cet utilisateur n'a pas été supposé mais **demandé** : une première connexion sous le rôle le crée, un `SELECT current_user` donne son nom exact, et les `GRANT` portent sur ce nom. Les tables témoins `_naif` sont volontairement laissées hors de portée — contrôle négatif inclus.

### 16.5 Résultats mesurés

| Tâche | Exécution 1 | Exécution 2 | Écart |
|---|---|---|---|
| 07 ingestion `music_info` | 151,8 s | 182,0 s | + 30,2 s |
| 08 ingestion `listening_history` | 91,1 s | 91,2 s | **+ 0,1 s** |
| 09 porte de qualité | 91,1 s | 91,2 s | **+ 0,1 s** |
| 10 feature engineering | 106,2 s | 107,3 s | + 1,1 s |
| 11 chargement Redshift | — | 17,0 s | — |
| **Total** | 7 min 22 s | **8 min 11 s** | |

**La reproductibilité est le résultat le plus solide.** Trois tâches sur quatre retombent à la seconde près d'une exécution à l'autre, à un jour d'intervalle. Seule la première varie, et la cause est identifiée : elle absorbe le redémarrage de l'application EMR restée inactive une nuit.

**Le pool a sérialisé ce que le graphe autorisait.** `08` démarre systématiquement 0,7 seconde après la fin de `07`, jamais en même temps.

**Latences d'ordonnancement** : 0,73 · 0,55 · 0,64 · 0,58 s. Négligeables devant des tâches de plus d'une minute, ce qui valide le grain de découpage.

### 16.6 Le coût est piloté par le nombre de soumissions

Les quatre jobs EMR sont facturés **à l'identique** : 0,233 vCPU-heure et 1,05 Go-heure chacun, alors qu'ils durent de 91 à 182 secondes. Ces 0,233 vCPU-heure correspondent à 14 vCPU pendant exactement soixante secondes, soit le **minimum de facturation d'une minute** d'EMR Serverless.

Autrement dit : hacher 9,7 millions de lignes coûte exactement le même prix qu'en lire 50 000. **Le coût dépend du nombre de soumissions, pas du volume traité.** Découper `10_feature_engineering` en trois tâches — « jointure », « calcul de la cible », « variables d'artiste » — aurait triplé la facture de cette étape pour zéro calcul supplémentaire, en plus d'ajouter trois secondes de latence d'ordonnancement.

C'est un chiffre dur à opposer à l'intuition répandue selon laquelle un pipeline plus finement découpé est toujours plus observable.

### 16.7 Chargement de l'entrepôt

Le SQL exécuté par la cinquième tâche est un fichier versionné, découpé en instructions par la convention `-- @ Libellé` déjà utilisée par `infra/rs.sh` : le journal Airflow affiche « Charger dim_track… » plutôt qu'un identifiant opaque.

Les 17 secondes mesurées sont **majoritairement de la granularité de scrutation** — cinq instructions interrogées toutes les deux secondes — et non du temps Redshift. Le `COPY` de 9,7 millions de lignes avait été chronométré à 3,1 secondes au benchmark.

**Trois limites assumées et documentées dans le fichier SQL :**

`TRUNCATE` valide implicitement. Si le `COPY` échoue derrière, la table reste vide jusqu'au prochain chargement réussi. C'est accepté parce que le contrôle de volumétrie final fait échouer la tâche immédiatement, rendant la situation visible. Un contexte de production chargerait en table tampon puis permuterait par `ALTER TABLE RENAME` dans une transaction.

`ANALYZE` n'est pas exécuté : l'instruction exige d'être propriétaire de la table. Transférer la propriété à l'orchestrateur pour ce seul besoin lui donnerait au passage le droit de supprimer les tables. L'analyse automatique de Redshift prend le relais.

Les tables témoins `_naif` ne sont pas rechargées : elles n'ont servi qu'à l'étude de benchmark.

### 16.8 Reste ouvert

**Le renouvellement des jetons est manuel**, toutes les quatre heures au maximum. Acceptable pour un orchestrateur déclenché à la main ; une exécution planifiée exigerait un rafraîchissement automatique, par exemple un conteneur compagnon qui réécrit les variables d'environnement avant expiration.

**Le DAG n'a pas de périodicité** (`schedule=None`). Les données sources ne portent aucun horodatage, donc aucun intervalle n'aurait de sens. Le jour où des données arriveraient en continu, le déclenchement se ferait sur l'arrivée de fichiers plutôt que sur une horloge.

---

## 17. Tâche 21 — Surveillance CloudWatch

### 17.1 Ce que la compétence exige

Le brief ne vise que deux compétences, et celle-ci en est une :

> **C2.7** — Mettre en place des outils de surveillance pour suivre les performances de l'infrastructure de données, identifier les problèmes potentiels et optimiser les systèmes, **en vue d'une gestion proactive**.

Trois verbes, trois exigences distinctes : *suivre* appelle un tableau de bord, *identifier* appelle des alarmes, *optimiser* appelle une mesure qui débouche sur une décision. Le mot « proactive » écarte la surveillance qui constate après coup.

### 17.2 Principe de construction : aucune dimension supposée

CloudWatch exige un jeu de dimensions **exact et complet**. Une alarme posée sur un jeu incomplet reste indéfiniment en `INSUFFICIENT_DATA` ; posée sur un jeu inexistant avec `treat-missing-data: notBreaching`, elle affiche `OK` sans rien mesurer.

Le script interroge donc `list-metrics` pour chaque métrique, retient le jeu réellement publié qui désigne les ressources du projet, et **vérifie qu'il porte des données avant de créer quoi que ce soit**.

Une vérification indirecte est nécessaire pour les métriques d'incident. `FailedJobs` n'a légitimement aucune donnée tant qu'aucun job n'a échoué : son jeu de dimensions est donc validé au moyen de `SuccessJobs`, qui partage exactement le même jeu et doit, lui, porter des points. Cette astuce n'est valable qu'entre métriques de schéma identique — la tentative de l'étendre à Redshift a produit l'incident 10.15.

Le premier diagnostic a d'ailleurs confirmé le risque : `SuccessJobs` filtrée sur `ApplicationId` seul ne renvoie **aucun point**, alors que quatre jobs avaient réussi. Le jeu publié est `ApplicationId + ApplicationName`.

### 17.3 Alarmes retenues

| Alarme | Métrique | Seuil | Ce qu'elle anticipe |
| --- | --- | --- | --- |
| `soundlab-emr-jobs-en-echec` | `FailedJobs` | ≥ 1 sur 5 min | Échec d'un traitement distribué |
| `soundlab-emr-quota-vcpu-proche` | `CPUAllocated` | ≥ 15 vCPU | Le `ServiceQuotaExceededException` de l'incident 10.3, **avant** qu'il ne survienne |
| `soundlab-redshift-quota-bientot-atteint` | `UsageLimitAvailable` | ≤ 12 RPU-h restantes | La désactivation automatique du groupe de travail par le plafond dur |

Les deux dernières sont celles qui incarnent le mot « proactive ». Le quota de 16 vCPU et le plafond de 60 RPU-heures sont des protections qui **coupent sans prévenir** ; ces alarmes transforment une coupure subie en décision anticipée.

Une quatrième alarme, sur les requêtes en échec, a été retirée — voir l'incident 10.15.

### 17.4 Tableau de bord

Six widgets, dont un mérite d'être signalé : **ressources réservées contre ressources réellement utilisées**, qui superpose `WorkerCpuAllocated` et `WorkerCpuUsed`. Il répond à la partie « optimiser les systèmes » de la compétence : si l'on réserve 14 vCPU et qu'on en consomme 4, le dimensionnement est à revoir — et c'est une mesure, pas une intuition.

Ce widget utilise une expression de recherche agrégeant tous les `JobId`, qu'il est impossible d'énumérer à l'avance puisque chaque exécution en crée de nouveaux.

### 17.5 Vérification du dispositif lui-même

Un mode `--tester` force une alarme en état `ALARM` puis la remet en `OK`, ce qui déclenche réellement la chaîne alarme → SNS → courriel. **La notification a été reçue**, ce qui distingue un dispositif vérifié d'une configuration qu'on espère juste.

C'est la même exigence que pour les tests négatifs du rôle IAM : une alarme dont on n'a jamais constaté qu'elle alerte n'est pas démontrée.

### 17.6 Ce qui n'est pas couvert

**La dérive des données** (*data drift*) n'est pas surveillée — c'est l'objet de la tâche 15, non réalisée. Les contrôles de qualité de la tâche 9 vérifient la conformité structurelle à chaque exécution, pas l'évolution des distributions dans le temps.

**Airflow lui-même n'est pas sous CloudWatch**, puisqu'il tourne en conteneur local. Son historique d'exécution et ses journaux de tâches tiennent lieu de surveillance d'orchestration. Un déploiement MWAA publierait ces métriques nativement — c'est un des rares avantages concrets qu'on abandonne avec ce choix, et il est mince devant l'écart de coût.

**Aucune métrique métier personnalisée** n'est publiée. Le job de qualité produit un rapport JSON dans S3 ; le faire émettre une métrique CloudWatch — nombre de contrôles bloquants en échec, taux de couverture — permettrait d'alarmer sur la qualité des données et pas seulement sur la santé de l'infrastructure. C'est la première extension à recommander.

### 17.7 Coût

Un tableau de bord et trois alarmes standard restent dans l'offre gratuite : AWS en inclut trois et dix respectivement par compte. La surveillance n'ajoute donc rien à la facture du projet.

---

## 18. Restitution de l'entrepôt vers le lac (UNLOAD)

*Extension de la tâche 9.5 « Entrepôt Redshift », et non une tâche distincte du planning.*

### 18.1 Ce que la tâche démontre

La compétence **C2.4** demande d'éprouver la structure de base de données sur la performance, la sécurité, l'évolutivité et le volume. Les tâches précédentes l'ont chargée et mesurée en lecture. Celle-ci ferme le cycle : l'entrepôt ne se contente pas d'absorber la donnée, il **produit un objet métier et le restitue au lac**, dans un format que le reste de la chaîne relit sans dépendre de lui.

### 18.2 Quel agrégat, et pourquoi celui-là

Un **référentiel d'artistes** : nombre de titres, écoutes cumulées, écoutes médianes, nombre et taux de hits, auditeurs cumulés, bornes d'années, genre principal.

Le choix n'est pas décoratif. L'analyse SHAP de la tâche 12 a montré que le signal prédictif se concentre sur l'artiste, pas sur les descripteurs audio. L'entrepôt produit donc le référentiel de l'objet que le modèle a désigné comme central — la boucle se referme sur elle-même.

C'est aussi exactement la forme de calcul pour laquelle un entrepôt colonnaire existe : agrégation sur une clé de faible cardinalité, jointure sur la `DISTKEY` commune, aucune donnée à sortir du cluster. Le même calcul en Spark impliquerait de relire les Parquet depuis S3 et de payer un *shuffle* complet.

**Mesure : 3,2 s de calcul Redshift** pour la jointure en étoile entre 50 683 lignes de dimension et 9 711 301 lignes de faits, incluant un `COUNT(DISTINCT)` par piste, l'export et les contrôles.

### 18.3 Où écrire, et pourquoi pas là où c'était déjà permis

Le rôle `SoundLabRedshiftS3Role` disposait déjà de `s3:PutObject` sur `soundlab-logs-558852/redshift-unload/*`. Utiliser ce préfixe aurait évité toute modification de politique. Trois raisons de ne pas le faire :

- le compartiment de journaux porte une **règle d'expiration à 365 jours**, qui effacerait l'export ;
- il est chiffré en **AES256 et non par la clé du projet**, donc hors du périmètre décrit dans l'AIPD ;
- une donnée métier rangée dans un compartiment de journaux est un **classement fautif**, et il se voit.

L'export vise donc `s3://soundlab-curated-558852/exports_entrepot/referentiel_artistes/`, chiffré par la CMK du projet et sans expiration prématurée.

> La facilité d'une permission déjà accordée n'est pas un argument de conception. Elle décide seulement de l'effort, pas de la justesse.

### 18.4 Le droit d'écrire, et sa limite

`infra/07_droits_unload.sh` ajoute une déclaration unique : `s3:PutObject` **et** `s3:DeleteObject` sur le seul préfixe `exports_entrepot/*`. La suppression n'est pas un excès de zèle — elle est requise par l'option `CLEANPATH` d'`UNLOAD`, qui vide la destination avant d'écrire ; sans elle le second export échouerait sur *path is not empty*.

Le script attend la propagation en interrogeant `simulate-principal-policy` jusqu'à obtenir la décision publiée, puis exécute un contrôle négatif : l'écriture dans `curated/music_info/` doit rester refusée. Elle l'est — `implicitDeny`.

**Limite du dispositif, énoncée avant de s'en servir.** La simulation a répondu `allowed` dès la première tentative. C'est attendu : `simulate-principal-policy` lit le *document* de politique, cohérent immédiatement après écriture, tandis que ce qui peut retarder est l'autorisateur de S3 lui-même, que la simulation ne mesure pas. Cette porte prouve que **la politique est juste**, pas que **S3 l'applique déjà**. La preuve réelle est l'`UNLOAD`, qui a réussi.

### 18.5 Une vue plutôt qu'une requête dans le script

La définition du hit — 592 écoutes cumulées — est une règle métier. Dans un script d'export elle se dédouble : le modèle a la sienne, l'entrepôt la sienne, et elles divergent sans que personne s'en aperçoive. Placée dans la vue `soundlab.v_referentiel_artistes`, elle est déclarée une fois, dans l'entrepôt, et commentée par un `COMMENT ON VIEW`.

Le seuil y est **écrit en clair, et non recalculé**. Le recalculer ferait dériver la définition du hit dès qu'une écoute est ajoutée, et le référentiel cesserait de décrire le même objet que le modèle.

Un contrôle le vérifie à chaque exécution, et c'est là que la rigueur se joue : Spark a obtenu le seuil par `approxQuantile(0.75, erreur relative 0)`, qui renvoie une valeur **réellement présente** dans les données. `PERCENTILE_DISC` a la même sémantique, `PERCENTILE_CONT` interpole. Comparer avec la mauvaise des deux aurait produit un écart sans signification, attribué à tort à une divergence de population.

| Contrôle | Attendu | Obtenu |
| --- | --- | --- |
| Couverture de la jointure | 60,1 % (journal Spark, tâche 10) | **60,10 %** |
| Seuil `PERCENTILE_DISC(0,75)` | 592 (approxQuantile) | **592** |
| Seuil `PERCENTILE_CONT(0,75)` | ~592, interpolation possible | **592,0** |
| Taux de hits global | ~25 % par construction d'un P75 | **25,01 %** |

Les deux percentiles coïncident : le quartile tombe sur un palier de valeurs identiques, l'interpolation n'avait rien à interpoler.

### 18.6 `PARALLEL OFF` — la leçon de `dim_track_naif`, transposée

Sans cette option, Redshift écrit **un fichier par tranche de calcul**. Quelques milliers de lignes donneraient des dizaines d'objets de quelques kilo-octets — la même erreur de granularité que la table témoin `dim_track_naif` du benchmark, déplacée du stockage en colonnes vers le stockage objet.

Avec `PARALLEL OFF`, le tri est en outre **global** : le fichier est directement exploitable sans tri côté lecteur.

**Résultat : un objet, `000.parquet`, 164,2 KiB pour 6 207 artistes.**

### 18.7 Un défaut que seul le regard a trouvé

Voir l'incident **10.16**. La première version exportait correctement un référentiel dont la colonne genre était vide pour 47 % des artistes, dont cinq des quinze plus écoutés. Aucun contrôle automatique ne l'a signalé — tous étaient au vert. La correction (`FIRST_VALUE(genre IGNORE NULLS)` et normalisation des chaînes vides) a récupéré 978 artistes ; 1 959 restent sans genre, et c'est irréductible.

La vue expose désormais `nb_titres_avec_genre`, qui dit sur quelle assise repose la valeur : The Black Keys sort `rock` sur 8 titres de 53, Coldplay sur 28 de 43. Ces deux valeurs ne se lisent pas de la même façon, et le référentiel le dit au lieu de le taire.

### 18.8 Ce qui n'est pas fait

**L'export n'est pas orchestré.** Il s'exécute à la main via `infra/rs.sh`. L'ajouter au DAG comme sixième tâche, après le chargement Redshift, serait cohérent — le rôle `SoundLabAirflowRole` dispose déjà de `redshift-data` sur le groupe de travail, mais pas du `PassRole` vers `SoundLabRedshiftS3Role` que l'`UNLOAD` requiert. C'est une extension d'une seule déclaration IAM, délibérément laissée hors périmètre pour ne pas élargir les droits de l'orchestrateur sans nécessité démontrée.

**Le fichier exporté n'est pas déclaré au catalogue Glue.** Les autres jeux de données du lac le sont. Le déclarer rendrait le référentiel interrogeable par Athena sans réveiller Redshift — c'est la première extension à recommander après celle-ci.

**Aucune version n'est conservée.** `CLEANPATH` écrase l'export précédent. Pour un référentiel recalculé à la demande c'est le comportement voulu ; pour un suivi dans le temps il faudrait partitionner par date d'exécution.

### 18.9 Fichiers

| Fichier | Rôle |
| --- | --- |
| `infra/07_droits_unload.sh` | Étend la politique du rôle Redshift au préfixe d'export, attend la propagation, contrôle négatif |
| `sql/03_unload_referentiel.sql` | Contrôles de population et de seuil, vue `v_referentiel_artistes`, aperçu, `UNLOAD`, contrôle de volumétrie |

---

## 19. Tâche 22 — Le rapport de certification

### 19.1 Une ambiguïté levée avant d'écrire

La fiche Notion de la tâche 22 décrivait la mise à jour du **document de cadrage Jedha** — métriques réelles et grille d'autocontrôle des treize compétences. Ce document avait déjà été rendu et validé ; sa consigne était donc périmée.

Ce qui restait à produire était le **livrable n° 4 du brief Bloc 6** : « un rapport complet décrivant chaque étape du projet, les résultats obtenus, et les recommandations d'optimisation ». Deux documents distincts, deux lecteurs distincts, deux structures distinctes.

> Une tâche dont l'intitulé ne décrit plus le travail à faire est pire qu'une tâche absente : elle donne l'illusion que le sujet est couvert. La fiche a été renommée plutôt que simplement cochée.

### 19.2 La structure vient du brief, pas de moi

Le brief énonce cinq critères d'évaluation, et ils recouvrent exactement les cinq étapes du projet. Le rapport suit donc ces cinq critères, et chaque section s'ouvre sur un encadré rappelant celui qu'elle sert.

| § | Section | Critère du brief |
| --- | --- | --- |
| 2 | Architecture conçue | Pertinence de l'architecture |
| 3 | Pipeline PySpark distribué | Performance du pipeline |
| 4 | Entrepôt Redshift | Optimisation Redshift — cœur de **C2.4** |
| 5 | Sécurité et gestion des accès | Sécurisation des données |
| 6 | Surveillance et gestion proactive | **C2.7** |
| 7 | Performances et 14 recommandations | Exigé explicitement par le brief |
| 10 | Couverture critères × sections × preuves | Lecture du jury |

Le § 10 est la table que le jury lira en premier : chaque exigence des énoncés C2.4 et C2.7 y est mise en face d'une section et d'une preuve chiffrée.

### 19.3 L'écart au brief, traité de front

Le brief nomme **Hadoop et Databricks** ; la réalisation retient S3 et EMR Serverless. Le § 2.3 du rapport y consacre trois sous-sections plutôt qu'une note de bas de page.

L'argument central sur Hadoop n'est pas que HDFS soit dépassé, mais que le problème qu'il résout — la localité des données — a été vidé de sa substance par trois évolutions, et surtout que **HDFS impose de garder les machines allumées pour garder les données**. Le pipeline tourne huit minutes par jour : un cluster dimensionné pour ces 2 Gio afficherait un taux d'utilisation de **0,6 %**.

L'argument central sur Databricks n'est pas le prix mais l'absence de destinataire : les notebooks collaboratifs supposent des collaborateurs, Delta Lake résout les écritures concurrentes qui n'existent pas ici, Unity Catalog gouverne des accès multi-équipes. Et le § 2.3.3 énumère ce que l'écart fait perdre — le metastore Hive et le *bucketing*, les notebooks, le voyage dans le temps.

> Un écart chiffré et assumé se défend. Un écart passé sous silence se paie.

### 19.4 Le rapport comme artefact reproductible

`node rapport/build_rapport.js` régénère le document à l'identique — même principe que le diaporama (§ 15).

Le point notable est le **sommaire auto-numéroté**. Une première passe produit le PDF ; `pages.py` en extrait la page de début de chaque section dans `pages.json` ; la seconde passe l'injecte. Le sommaire occupant une page dans les deux cas, la pagination est identique avec ou sans numéros, donc l'opération converge en deux passes. **Aucun numéro de page n'est saisi à la main, donc aucun ne peut devenir faux.**

La numérotation des légendes suit la même logique : `TCAP` et `FCAP` incrémentent un compteur au lieu de porter un numéro écrit en dur. Ajouter un tableau au milieu du document ne peut plus décaler la suite.

### 19.5 Relecture visuelle par un tiers

Le document a été rendu en 38 images et relu deux fois par un relecteur distinct de l'auteur, sur la seule mise en page.

| Défaut trouvé | Nature |
| --- | --- |
| Figure du DAG débordant de son cadre | Largeur de bloc de code non vérifiée |
| Tableau de la porte de qualité à 12 lignes pour 13 contrôles annoncés | Deux contrôles fusionnés sur une ligne |
| Six tableaux sans légende | Oubli, invisible à la relecture du code |
| Identifiants hachés, une lettre seule sur une ligne | Colonnes trop étroites |
| Page blanche en milieu de document | Saut de page explicite après un contenu finissant en bas de page |
| Renvois « § X.Y » décalés d'une sous-section | Numérotation écrite de mémoire |

Deux corrections sont structurelles et non ponctuelles : les sauts de page sont désormais portés par le titre lui-même (`pageBreakBefore`) au lieu d'être des paragraphes vides, ce qui rend une page blanche impossible ; et les légendes sont auto-numérotées.

> Le dernier défaut de la liste est le plus instructif : j'avais écrit « § 4.8 » alors que la section 4 s'arrête à 4.7. J'avais numéroté de mémoire au lieu d'aller lire — le réflexe exact que le § 8 du rapport dénonce. L'incident 10.16 vaut aussi pour son auteur.

### 19.6 Ce qui reste perfectible

Trois pages de fin de section sont remplies à 15-25 %, conséquence directe du choix de faire démarrer chaque section sur une page neuve. Supprimer les sauts densifierait le document au prix de la navigation ; l'arbitrage a été tranché en faveur du jury, qui navigue au sommaire.

Le schéma d'architecture occupe une page en orientation paysage. Même agrandi, sa typographie interne reste petite — c'est le diagramme vectoriel, livrable n° 1, qui se lit à n'importe quelle échelle.
