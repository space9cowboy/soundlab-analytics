# Analyse d'impact relative à la protection des données (AIPD / DPIA)

**Traitement** : « Prédiction du potentiel commercial d'un titre musical »
**Responsable de traitement** : SoundLab Analytics SAS (société fictive), Lyon
**Rédacteur** : Loïc Rabetsanta, architecte data
**Version** : 1.0
**Statut** : soumise à l'avis du DPO

> **Portée de ce document.** Il s'agit d'un exercice de certification portant sur une organisation fictive et sur un jeu de données public de recherche. Les analyses juridiques qui suivent appliquent la méthodologie CNIL et le RGPD à cette situation simulée ; elles ne constituent pas un avis juridique.

---

## Sommaire

1. [Le traitement](#1-le-traitement)
2. [Nécessité et proportionnalité](#2-nécessité-et-proportionnalité)
3. [Mesures protectrices des droits des personnes](#3-mesures-protectrices-des-droits-des-personnes)
4. [Mesures de sécurité](#4-mesures-de-sécurité)
5. [Appréciation des risques](#5-appréciation-des-risques)
6. [Risques résiduels et plan d'action](#6-risques-résiduels-et-plan-daction)
7. [Conclusion et avis](#7-conclusion-et-avis)
8. [Annexe — correspondance avec le référentiel RNCP](#annexe--correspondance-avec-le-référentiel-rncp)

---

## 1. Le traitement

### 1.1 Finalité

SoundLab Analytics fournit à des labels indépendants un service d'aide à la décision : estimer, avant investissement promotionnel, la probabilité qu'un titre atteigne le quartile supérieur des écoutes de son catalogue.

Le traitement consiste à entraîner et exploiter un modèle de classification binaire (forêt aléatoire) à partir de caractéristiques audio et d'indicateurs d'engagement agrégés.

**La finalité ne porte pas sur les personnes.** Aucune décision, aucun profilage, aucun ciblage n'est produit à l'égard d'un auditeur. Les données d'écoute ne servent qu'à construire un indicateur agrégé par *titre* — nombre d'auditeurs distincts et volume d'écoutes. Les personnes concernées sont une source de mesure, jamais une cible.

Cette caractéristique est déterminante pour la suite de l'analyse : elle exclut l'application de l'article 22 du RGPD (décision individuelle automatisée) et réduit fortement la gravité des impacts potentiels.

### 1.2 Nature des données traitées

Source unique : jeu de données public `undefinenull/million-song-dataset-spotify-lastfm` (Kaggle), sous licence CC BY-NC 4.0, dérivé du Million Song Dataset et du Echo Nest Taste Profile.

| Donnée | Nature | Volume | Statut |
| --- | --- | --- | --- |
| `user_id` (source) | Identifiant d'écoute, **déjà haché à la source** | 962 037 valeurs distinctes | Donnée à caractère personnel pseudonymisée |
| `track_id` | Identifiant de titre | 30 459 valeurs distinctes | Donnée non personnelle |
| `playcount` | Nombre d'écoutes d'un titre par un identifiant | 9 711 301 lignes | Donnée d'usage rattachée à une personne |
| Métadonnées et caractéristiques audio | Titre, artiste, genre, 13 variables Spotify | 50 683 titres | Données non personnelles |

**Aucune donnée d'identification directe** — nom, adresse électronique, adresse IP, identifiant d'appareil, coordonnées de géolocalisation — n'entre dans le périmètre. Aucune donnée relevant de l'article 9 du RGPD n'est collectée directement.

**Point de vigilance assumé.** Un historique d'écoute peut *indirectement* révéler une orientation religieuse ou une opinion politique — musique liturgique, chants engagés. Cette possibilité n'est pas exploitée par le traitement, dont les variables explicatives sont exclusivement audio et dont les indicateurs d'engagement sont agrégés par titre. Le risque est néanmoins consigné à la section 5.

### 1.3 Double pseudonymisation

Une particularité de ce traitement mérite d'être soulignée, car elle en modifie l'analyse de risque.

Les identifiants du Echo Nest Taste Profile **sont déjà des condensats** : le jeu de données publié ne contient aucun identifiant en clair, et la table de correspondance vers les comptes réels n'a jamais été rendue publique. Elle est détenue, le cas échéant, par l'éditeur d'origine.

SoundLab applique par-dessus une **seconde pseudonymisation**, avec un sel qui lui est propre.

Conséquence : **SoundLab ne détient à aucun moment, et ne peut reconstituer par aucun moyen à sa disposition, le lien entre un jeton et une personne physique.** Ni le sel, ni la table de correspondance d'origine, ni aucune donnée directement identifiante ne se trouvent dans son système d'information. Cette impossibilité n'est pas une politique interne révocable : elle est structurelle.

### 1.4 Destinataires

| Destinataire | Données accessibles | Fondement |
| --- | --- | --- |
| Équipe data de SoundLab (1 personne) | Couche `curated` pseudonymisée | Exécution du traitement |
| Labels clients | Scores agrégés par titre, sans aucune donnée d'écoute | Objet du contrat de service |
| Amazon Web Services (sous-traitant) | Données chiffrées au repos et en transit | Contrat AWS, incluant les clauses contractuelles types |

**Aucun transfert hors de l'Espace économique européen.** L'ensemble des ressources est provisionné dans la région `eu-north-1` (Stockholm, Suède). Chaque script d'infrastructure refuse de s'exécuter si la région effective diffère — le cantonnement géographique est vérifié par le code, non par une consigne.

### 1.5 Durées de conservation

| Catégorie | Durée retenue | Purge technique | Effacement effectif | Justification |
| --- | --- | --- | --- | --- |
| Données brutes (`raw`) | 12 mois | 30 jours | 395 jours | Permet de rejouer l'ingestion après correction d'un défaut de pipeline |
| Données préparées (`curated`) | 24 mois | 30 jours | 760 jours | Couvre deux cycles annuels de réentraînement et l'analyse de dérive |
| Modèles et artefacts | Illimitée | — | — | Ne contiennent aucune donnée à caractère personnel |
| Journaux CloudTrail | 12 mois | 30 jours | 395 jours | Traçabilité des accès, aligné sur la recommandation CNIL en matière de journalisation |
| Rapports de qualité et d'audit | 36 mois | — | — | Preuve de conformité opposable |

**Mise en œuvre technique — appliquée** par le script `infra/03_retention.sh`, versionné dans le dépôt.

**Pourquoi deux colonnes de délai.** Les buckets sont versionnés, ce qui protège d'un écrasement accidentel mais modifie la sémantique de l'expiration : sur un bucket versionné, une règle `Expiration` **ne supprime pas l'objet**. Elle pose un marqueur de suppression et bascule la version courante en version non courante. L'objet disparaît des listages, mais la donnée reste intégralement récupérable — et demeure donc une donnée à caractère personnel au sens du RGPD.

L'effacement n'est réel qu'à l'expiration de la version non courante, réglée à 30 jours. Une règle de cycle de vie dépourvue de `NoncurrentVersionExpiration` produirait une conformité apparente et une conservation effectivement illimitée.

Configuration vérifiée :

| Bucket | Expiration | Purge des versions non courantes |
| --- | --- | --- |
| `soundlab-raw-558852` | 365 j | 30 j |
| `soundlab-curated-558852` | 730 j | 30 j |
| `soundlab-logs-558852` | 365 j | 30 j |
| `soundlab-models-558852` | aucune | 30 j |
| `soundlab-scripts-558852` | aucune | 30 j |

---

## 2. Nécessité et proportionnalité

### 2.1 Base légale

**Intérêt légitime** — article 6.1.f du RGPD.

La mise en balance exigée par ce fondement s'établit comme suit.

*Intérêt poursuivi.* Permettre à des labels indépendants, structures à faibles moyens, d'allouer un budget promotionnel restreint là où il a le plus de chances de produire un effet. L'intérêt est économique, légitime et réel.

*Nécessité.* L'indicateur `unique_listeners` — nombre d'auditeurs distincts par titre — est la variable la plus prédictive du modèle. Sa suppression fait chuter l'AUC-ROC de **0,9921 à 0,5959**, c'est-à-dire au niveau du hasard. Cette mesure a été effectuée : la variante V2, restreinte aux seules caractéristiques audio, est documentée et reproductible. La nécessité du traitement des données d'écoute n'est donc pas postulée, elle est **démontrée expérimentalement**.

*Mise en balance.* Les droits et libertés des personnes concernées sont faiblement affectés : les données sont doublement pseudonymisées, agrégées par titre, aucune décision ne les concerne, et le responsable de traitement est dans l'impossibilité structurelle de les ré-identifier. L'atteinte est minime au regard de l'intérêt poursuivi.

*Attente raisonnable.* Les personnes concernées ont contribué à un jeu de données publié à des fins de recherche par son éditeur d'origine. Une réutilisation analytique agrégée s'inscrit dans le prolongement de cette publication.

### 2.2 Minimisation — article 5.1.c

Trois mesures concrètes, toutes vérifiables :

**Suppression de l'identifiant en clair.** La colonne `user_id` source est supprimée du jeu de données avant toute écriture en couche `curated`. Une assertion dans le code d'ingestion arrête le job si elle subsiste.

**Contrôle automatisé de non-régression.** La suite de tests de la tâche 9 comporte un contrôle bloquant qui vérifie l'absence de toute colonne d'identification directe (`user_id`, `email`, `ip`…) dans la couche `curated`. Résultat : **conforme**.

**Réduction du condensat au strict nécessaire.** Le jeton est tronqué à 128 bits, longueur suffisante pour garantir l'unicité sur 962 037 personnes sans conserver d'information superflue.

### 2.3 Exactitude — article 5.1.d

Treize contrôles de qualité sont exécutés sur la couche `curated` par un job dédié, dont le code de sortie est non nul en cas d'échec bloquant. Branché dans l'orchestrateur, ce job empêche la propagation de données invalides vers le modèle.

Résultats : **13 contrôles sur 13 réussis, aucun échec bloquant, aucun avertissement.**

| Contrôle | Résultat |
| --- | --- |
| Unicité de la clé `track_id` | 0 doublon, 0 nul sur 50 683 |
| Complétude des 13 caractéristiques audio | 0 % de valeurs nulles |
| Respect des bornes de l'API Spotify | conforme |
| Absence d'identifiant direct en couche curated | conforme |
| Format du jeton pseudonyme | 32 caractères hexadécimaux, homogène |
| Unicité de la clé `(user_id_hash, track_id)` | 0 doublon |
| Positivité de `playcount` | min 1, max 2 948 |
| Intégrité référentielle vers `music_info` | 0 orphelin sur 30 459 |
| Couverture du catalogue | 60,1 % |

---

## 3. Mesures protectrices des droits des personnes

### 3.1 Information — articles 13 et 14

Les données n'ayant pas été collectées auprès des personnes concernées, l'article 14 s'applique. Son paragraphe 5.b prévoit une exception lorsque l'information se révèle impossible ou exige des efforts disproportionnés — ce qui est le cas ici, SoundLab n'ayant aucun moyen de joindre les personnes.

**Mesure compensatoire retenue** : publication d'une notice d'information sur le site de SoundLab, décrivant la source des données, la finalité, la base légale, les durées de conservation et les modalités d'exercice des droits. C'est la mesure explicitement recommandée par le même article.

### 3.2 Droits d'accès, de rectification et d'effacement — le cas de l'article 11

**L'article 11 du RGPD s'applique intégralement à ce traitement.** Lorsque le responsable de traitement n'est pas en mesure d'identifier une personne concernée, il n'est pas tenu de conserver ou d'obtenir des informations supplémentaires dans le seul but de se conformer au règlement, et les articles 15 à 20 ne s'appliquent pas — sauf si la personne fournit elle-même les éléments permettant son identification.

C'est précisément la situation décrite au § 1.3 : SoundLab ne peut, par aucun moyen à sa disposition, relier un jeton à une personne physique.

**Mécanisme mis en place au titre de l'article 11.2.** Une procédure documentée permet à une personne qui connaît son identifiant Taste Profile d'origine de faire valoir ses droits :

1. La personne transmet son identifiant source par un canal authentifié.
2. Le DPO — seul habilité à lire le secret — calcule `SHA-256(sel || identifiant)`, tronqué à 128 bits.
3. La requête est exécutée sur la couche `curated` à partir de ce jeton.
4. Selon la demande : extraction des lignes correspondantes (accès, article 15), ou suppression puis réécriture de la partition concernée (effacement, article 17).
5. L'opération est journalisée dans le registre des demandes, avec sa date et son issue.

Ce mécanisme est **techniquement réalisable en l'état** : le hachage est déterministe, `user_id_hash` est requêtable via Athena, et le format Parquet autorise la réécriture sélective. La suppression est effective dans les deux couches, `raw` incluse, cette dernière étant reconstruite par rejeu du pipeline sans la ligne visée.

*Délai cible : 30 jours, conformément à l'article 12.3.*

### 3.3 Droit d'opposition — article 21

Applicable, l'intérêt légitime étant la base légale retenue. Il s'exerce selon la même procédure que le droit d'effacement.

### 3.4 Droit à la portabilité — article 20

**Non applicable.** La portabilité suppose un traitement fondé sur le consentement ou sur un contrat, ce qui n'est pas le cas ici.

### 3.5 Absence de décision automatisée — article 22

**Non applicable.** Le modèle produit un score par *titre*. Aucune décision produisant des effets juridiques ou affectant significativement une personne physique n'est prise sur le fondement de ce traitement.

---

## 4. Mesures de sécurité

### 4.1 Chiffrement

| Mesure | Mise en œuvre |
| --- | --- |
| Chiffrement au repos | SSE-KMS avec clé gérée par le client `alias/soundlab` sur les buckets `raw`, `curated`, `models`, `scripts` |
| Rotation des clés | Rotation annuelle automatique activée |
| Chiffrement en transit | Politique de bucket refusant tout appel avec `aws:SecureTransport = false` |
| Optimisation | S3 Bucket Keys activées — réduit d'environ 99 % le nombre d'appels KMS sans affaiblir le chiffrement |

Le choix d'une clé gérée par le client plutôt que du chiffrement S3 par défaut apporte trois éléments requis par une démarche d'audit : une politique de clé lisible et opposable, une rotation maîtrisée, et une trace CloudTrail de **chaque opération de déchiffrement**.

### 4.2 Gestion du secret de pseudonymisation

Le sel est un secret de 32 octets généré localement par `openssl rand -hex 32`, transmis directement à l'API AWS Secrets Manager sans jamais être écrit sur disque, et chiffré au repos par la clé du projet.

Il est lu à l'exécution par le pilote Spark, via un rôle IAM dont l'autorisation `secretsmanager:GetSecretValue` est **bornée à ce seul secret**. Si le secret est inaccessible, le job s'interrompt avant tout traitement : mieux vaut ne rien produire que produire des données non pseudonymisées.

### 4.3 Contrôle d'accès

**Moindre privilège.** Le rôle d'exécution des traitements dispose de deux politiques en ligne strictement délimitées : lecture sur `raw`, `scripts` et `curated` ; écriture sur `curated`, `models` et `logs` ; opérations KMS limitées à la seule clé du projet ; lecture du seul secret de pseudonymisation ; journalisation restreinte au groupe `/aws/emr-serverless/*`. Aucun caractère générique sur les ressources, aucune politique administrative attachée à un rôle de calcul.

**Cloisonnement des couches.** Cinq buckets distincts plutôt qu'un seul, précisément pour permettre des politiques d'accès différenciées entre données brutes, données préparées, modèles, code et journaux.

**Absence de clé d'accès permanente.** L'authentification du poste d'administration repose sur `aws login` : flux OAuth 2.0 avec PKCE, identifiants temporaires renouvelés toutes les quinze minutes, valables douze heures au maximum. **Aucun secret persistant ne réside sur le poste de travail.** Cette mesure répond directement à un incident survenu en phase initiale du projet, au cours duquel des clés d'accès à durée illimitée avaient été exposées puis révoquées par AWS. La réponse retenue n'est pas une consigne de vigilance mais une modification d'architecture qui rend la classe d'incident impossible.

**Authentification renforcée.** Authentification multifacteur activée sur le compte racine et sur le compte d'administration. Aucune clé d'accès n'existe sur le compte racine.

**Exposition publique.** Blocage public intégral sur les cinq buckets — `BlockPublicAcls`, `IgnorePublicAcls`, `BlockPublicPolicy`, `RestrictPublicBuckets`.

### 4.4 Traçabilité

CloudTrail est actif en mode multirégion, avec **validation d'intégrité des fichiers journaux** — chaque fichier est signé, ce qui permet de détecter une altération a posteriori. Tout appel d'API sur les données, y compris chaque déchiffrement KMS, est enregistré.

Les journaux d'exécution des traitements sont collectés dans CloudWatch et archivés dans S3.

### 4.5 Disponibilité et intégrité

Versioning S3 activé sur l'ensemble des buckets : un écrasement accidentel est réversible. Durabilité annoncée par le service à 99,999999999 %, sur trois zones de disponibilité.

Manifeste d'empreintes SHA-256 des fichiers sources, versionné dans le dépôt de code : il établit que les données analysées sont exactement celles qui ont été téléversées.

---

## 5. Appréciation des risques

Méthodologie CNIL. Trois événements redoutés, évalués selon leur **gravité** (impact sur les personnes) et leur **vraisemblance** (probabilité de survenue), sur une échelle à quatre niveaux : négligeable, limitée, importante, maximale.

### 5.1 Accès illégitime aux données

**Scénario.** Un tiers non autorisé — attaquant externe, ou personne interne outrepassant ses droits — accède à la table `listening_history` et tente de reconstituer les habitudes d'écoute d'une personne identifiée.

**Sources de risque.** Compromission du poste d'administration ; erreur de configuration exposant un bucket ; fuite du secret de pseudonymisation.

**Impacts potentiels.** Révélation de goûts musicaux, susceptibles de laisser deviner une orientation religieuse ou politique.

**Mesures en place.** Double pseudonymisation avec sel non détenu conjointement aux données ; chiffrement KMS avec clé dédiée ; blocage public intégral ; TLS obligatoire ; moindre privilège ; absence de clé d'accès permanente ; authentification multifacteur ; journalisation CloudTrail à intégrité vérifiable.

| | Évaluation | Justification |
| --- | --- | --- |
| **Gravité** | **Limitée** | Sans le sel, le jeton n'est reliable à aucune personne. L'attaquant obtiendrait des séquences d'écoute rattachées à des identifiants opaques. |
| **Vraisemblance** | **Limitée** | Surface d'attaque réduite : aucun accès public, aucun secret permanent, un seul compte humain protégé par MFA. |

### 5.2 Modification non désirée des données

**Scénario.** Altération accidentelle ou malveillante des données d'écoute, conduisant à un modèle entraîné sur des valeurs faussées.

**Sources de risque.** Défaut de pipeline ; écrasement accidentel ; dérive silencieuse de la source.

**Impacts potentiels.** Impact direct sur les personnes : **nul**. Impact économique pour SoundLab et ses clients : recommandations erronées.

**Mesures en place.** Versioning S3 ; suite de treize contrôles de qualité faisant échouer le traitement en cas d'anomalie bloquante ; manifeste d'empreintes des sources ; couche `raw` conservée immuable, permettant de rejouer intégralement le pipeline ; schéma validé par nom de colonne et non par position, ce qui écarte le scénario d'une inversion silencieuse de deux colonnes.

| | Évaluation | Justification |
| --- | --- | --- |
| **Gravité** | **Négligeable** | Aucun effet sur les personnes concernées. |
| **Vraisemblance** | **Limitée** | Contrôles automatisés bloquants à chaque exécution. |

### 5.3 Disparition des données

**Scénario.** Perte de la couche `curated` ou des artefacts de modèle.

**Impacts potentiels.** Impact sur les personnes : **nul** — la disparition supprime le traitement, elle ne leur nuit pas. Impact opérationnel : interruption du service.

**Mesures en place.** Durabilité S3 ; versioning ; données brutes conservées ; ensemble du pipeline décrit dans des scripts idempotents et versionnés, permettant une reconstruction complète depuis la source.

| | Évaluation | Justification |
| --- | --- | --- |
| **Gravité** | **Négligeable** | Aucun préjudice pour les personnes concernées. |
| **Vraisemblance** | **Négligeable** | Réplication multi-zones assurée par le service. |

### 5.4 Synthèse

| Événement redouté | Gravité | Vraisemblance | Niveau |
| --- | --- | --- | --- |
| Accès illégitime | Limitée | Limitée | **Acceptable sous conditions** |
| Modification non désirée | Négligeable | Limitée | **Acceptable** |
| Disparition | Négligeable | Négligeable | **Acceptable** |

---

## 6. Risques résiduels et plan d'action

Un document d'analyse d'impact qui ne recense aucun risque résiduel est un document incomplet. Les cinq points suivants subsistent après application des mesures.

### R1 — Exposition du sel dans les plans d'exécution Spark

**Nature.** Le sel est injecté dans la requête sous forme de littéral. Il apparaît donc en clair dans le plan physique produit par Spark, susceptible d'être écrit dans les journaux d'exécution archivés sur S3.

**Portée.** Quiconque obtiendrait à la fois les journaux et la table `curated` pourrait tenter une attaque par force brute sur les identifiants sources.

**Mesures actuelles.** Bucket de journaux fermé à tout accès public, TLS obligatoire, accès limité au rôle d'exécution et au compte d'administration. Niveau de journalisation réglé sur `WARN`, ce qui n'imprime pas les plans en fonctionnement normal.

**Risque résiduel : limité.** Correction prévue — action A2.

### R2 — Singularisation par profil atypique

**Nature.** L'auditeur le plus actif du jeu de données compte **784 titres** dans son historique, contre une moyenne de 10. Un profil aussi distinctif est *singularisant* : un tiers disposant d'une source externe — un profil Last.fm public, par exemple — pourrait tenter un recoupement statistique et rattacher un jeton à une personne, sans avoir eu besoin du sel.

C'est le mode de ré-identification que la pseudonymisation, par construction, ne protège pas.

**Mesures actuelles.** Aucun horodatage n'est conservé, ce qui prive un attaquant de la dimension temporelle et rend le recoupement nettement plus difficile. Les données transmises aux clients sont agrégées par titre et ne contiennent aucune ligne d'écoute.

**Risque résiduel : limité.** Atténuation prévue — action A3.

### R3 — Inférence d'attributs sensibles

**Nature.** Les goûts musicaux peuvent laisser deviner une orientation religieuse ou politique. Bien que le traitement n'exploite pas cette information, elle est présente dans les données.

**Mesures actuelles.** Aucune variable de genre ou de tag n'entre dans le modèle retenu. Les données d'écoute ne servent qu'à produire des agrégats par titre.

**Risque résiduel : limité et assumé.** Il est inhérent à la nature de la donnée, non à son traitement.

### R4 — Absence de séparation des tâches

**Nature.** Un unique compte humain, `loic-admin`, détient la politique `AdministratorAccess`. Il n'existe ni séparation entre administration de l'infrastructure et accès aux données, ni principe des quatre yeux sur les opérations sensibles.

**Contexte.** Contrainte assumée d'un projet mené par une personne seule. Dans une organisation réelle, cette configuration serait inacceptable.

**Risque résiduel : important en contexte réel, accepté en contexte de projet.** Trajectoire cible — action A4.

### R5 — Durées de conservation non appliquées techniquement — ✅ **RÉSOLU**

**Nature du risque initial.** Les durées définies au § 1.5 n'étaient pas traduites en règles de cycle de vie S3 : aucun objet courant n'expirait automatiquement, et le document annonçait une limitation de conservation que l'infrastructure n'appliquait pas.

**Correction apportée.** Script `infra/03_retention.sh` : expiration à 365 jours sur `raw` et `logs`, 730 jours sur `curated`, purge des versions non courantes à 30 jours sur les cinq buckets, nettoyage des marqueurs de suppression sur les buckets sans expiration.

**Point technique relevé lors de la correction**, qui aurait pu produire une conformité de façade : sur un bucket versionné, `Expiration` ne supprime pas la donnée mais la bascule en version non courante. Sans `NoncurrentVersionExpiration`, la purge est illusoire. Les deux règles sont désormais posées conjointement.

**Risque résiduel : négligeable.**

### Plan d'action

| Réf. | Action | Priorité | Échéance / État |
| --- | --- | --- | --- |
| **A1** | Appliquer les règles d'expiration S3 conformes aux durées du § 1.5 | Haute | ✅ **Fait** — `infra/03_retention.sh` |
| **A2** | Sortir le sel des plans Spark, par une fonction définie par l'utilisateur portant le secret en fermeture plutôt qu'en littéral ; à défaut, chiffrer les journaux du bucket avec la clé du projet et restreindre leur lecture au DPO | Haute | Avant la mise en production |
| **A3** | Écarter du jeu d'entraînement les profils extrêmes au-delà du 99,9ᵉ percentile, ou plafonner le nombre de titres retenus par jeton | Moyenne | Tâche de préparation des variables |
| **A4** | Créer un rôle de lecture seule distinct du rôle d'administration ; réserver `AdministratorAccess` aux opérations d'infrastructure | Moyenne | Trajectoire cible |
| **A5** | Publier la notice d'information prévue au § 3.1 | Haute | Avant la mise en production |
| **A6** | Formaliser la procédure de notification de violation sous 72 heures — article 33 | Moyenne | Avant la mise en production |

**Vérification de A1, reproductible à tout moment :**

```bash
source .soundlab.env
for b in "$SL_B_RAW" "$SL_B_CUR" "$SL_B_LOG" "$SL_B_MOD" "$SL_B_SCR"; do
  printf '%-32s ' "$b"
  aws s3api get-bucket-lifecycle-configuration --bucket "$b" \
    --query 'Rules[0].{regle:ID,expiration:Expiration.Days,purge:NoncurrentVersionExpiration.NoncurrentDays}' \
    --output text
done
```

---

## 7. Conclusion et avis

Le traitement présente un **niveau de risque résiduel acceptable** pour les personnes concernées, sous réserve de la réalisation des actions A1, A2 et A5 avant toute mise en production.

Trois caractéristiques structurelles fondent cette appréciation :

**La finalité ne porte pas sur les personnes.** Aucune décision, aucun profilage individuel, aucun ciblage. Les données d'écoute alimentent un indicateur agrégé par titre.

**La ré-identification est structurellement impossible pour le responsable de traitement.** Double pseudonymisation, sel détenu séparément des données, table de correspondance d'origine jamais publiée. Cette impossibilité ne repose sur aucune politique interne révocable.

**La nécessité du traitement est démontrée, non postulée.** La suppression de la variable `unique_listeners` fait chuter la performance du modèle de 0,9921 à 0,5959 d'AUC-ROC — le niveau du hasard. La mise en balance exigée par l'article 6.1.f s'appuie sur une mesure reproductible.

**Consultation de l'autorité de contrôle — article 36 :** non requise, aucun risque résiduel élevé n'étant identifié après application des mesures.

| | |
| --- | --- |
| **Rédigé par** | Loïc Rabetsanta, architecte data |
| **Avis du DPO** | *En attente* |
| **Validation du responsable de traitement** | *En attente* |
| **Prochaine révision** | À chaque évolution substantielle du traitement, et au minimum tous les douze mois |

---

## Annexe — correspondance avec le référentiel RNCP

| Indicateur | Intitulé | Traité en |
| --- | --- | --- |
| 1.1 | Référence explicite aux réglementations | § 2, § 3 |
| 1.4 | Gestion des violations | Action A6 |
| 2.5 | Sécurité des données de l'architecture | § 4 |
| 3.1 | Conformité RGPD de l'architecture | ensemble du document |
| 3.3 | Confidentialité — chiffrement, droits d'accès | § 4.1, § 4.3 |
| 3.5 | Mécanismes de droit d'accès et de suppression | § 3.2 |
| 4.6 | Sécurité du pipeline — secrets, chiffrement, moindre privilège | § 4.2, § 4.3 |
| 5.1 | Conformité RGPD de la solution | ensemble du document |
| 5.2 | Loi Informatique et Libertés — exercice des droits | § 3.2, § 3.3 |
| 5.3 | Normes de sécurité — MFA, chiffrement | § 4.1, § 4.3 |
| 5.5 | IA éthique — vie privée, non-discrimination | § 1.1, § 5.1, R3 |

### Références

- Règlement (UE) 2016/679 (RGPD), notamment les articles 5, 6, 11, 12 à 22, 32, 33, 35 et 36
- CNIL, *La méthode d'analyse de risques*, guides AIPD
- Loi n° 78-17 du 6 janvier 1978 modifiée, dite Informatique et Libertés
- Comité européen de la protection des données, lignes directrices relatives à l'AIPD
