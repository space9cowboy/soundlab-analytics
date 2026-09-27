# Avenant n° 1 à l'AIPD — nouvelle source ListenBrainz et MusicBrainz

**Traitement** : « Prédiction du potentiel commercial d'un titre musical »
**Document amendé** : `01_dpia_analyse_impact.md`, version 1.0 du 10 septembre 2026, qui reste inchangé
**Version de l'avenant** : 1.0 — 24 septembre 2026
**Rédacteur** : Loïc Rabetsanta, architecte data
**Statut** : à soumettre à l'avis du DPO **avant la première ingestion**

> Même portée que le document amendé : exercice de certification sur une organisation fictive, sans valeur d'avis juridique.

## 1. Objet

Le chantier « les trois V » ajoute deux sources : les écoutes publiques de ListenBrainz (CC0 1.0, horodatées, JSON imbriqué) et les métadonnées MusicBrainz (CC0 pour les données principales, CC BY-NC-SA 3.0 pour les étiquettes). Il s'agit d'une évolution substantielle au sens du § 7 du document amendé : cet avenant la couvre.

## 2. Affirmations du document amendé qui ne valent pas pour la nouvelle source

| Document amendé | Situation avec ListenBrainz |
| --- | --- |
| § 1.2 : aucune donnée d'identification directe | Faux. Chaque écoute porte `user_name`, un pseudonyme public, et `user_id`, un entier. Tous deux sont présents à 100 % (inventaire du 24/09/2026 sur 3 200 écoutes). |
| § 1.3 : ré-identification structurellement impossible | Faux pour les lignes individuelles. Le dump source est public : une ligne pseudonymisée horodatée à la seconde se rapproche du dump par la suite des titres et des horodatages, **sans le sel**. |
| R2 : aucun horodatage conservé | Faux. L'horodatage est l'objet même du chantier. |
| § 2.1 : nécessité démontrée par la chute d'AUC de 0,9921 à 0,5959 sans `unique_listeners` | Démonstration caduque. Le groupe « engagement », dont fait partie cette variable, est écarté du modèle retenu pour fuite de cible. La nécessité est refondée au § 4 ci-dessous. |

## 3. Mesure structurante : l'agrégation dès la zone affinée

Décision du 24/09/2026 (option C) : **aucune ligne individuelle n'atteint la zone affinée.** Celle-ci ne contient que des agrégats par titre et par jour : le nombre d'écoutes et une esquisse HyperLogLog des auditeurs distincts.

Faisabilité vérifiée le 24/09/2026 sur l'EMR du projet (Spark 3.5.8) avec des données synthétiques : les esquisses se fusionnent correctement après écriture et relecture en Parquet, avec une erreur relative de 1,04 % en moyenne et de 3,15 % au maximum (précision lgK = 12). La fusion donne bien l'union des auditeurs et non leur somme : environ 6 000 attendus, contre 11 997 pour la somme naïve.

Chaîne de traitement :

| Étape | Données | Stockage |
| --- | --- | --- |
| Téléchargement du dump | Écoutes complètes, identifiants en clair | **Aucun** : flux en mémoire |
| Pseudonymisation en chemin | `user_id` remplacé par un jeton HMAC-SHA-256 salé, tronqué à 128 bits, avec un sel distinct de celui du Bloc 6. `user_name` supprimé. | — |
| Zone brute | Une ligne par écoute, pseudonymisée, horodatage à la seconde | 30 jours au plus |
| Zone affinée | Agrégats par titre et par jour, esquisses HLL | Durée du § 1.5 du document amendé (24 mois) |

## 4. Nécessité et proportionnalité

**Base légale inchangée** : intérêt légitime, article 6.1.f.

**Nécessité, refondée.** Les données d'écoute ne servent pas de variable explicative dans le modèle retenu (13 variables audio, 3 de contexte, 2 d'historique d'artiste). Elles servent à **construire la cible** : un titre est un succès s'il dépasse le 75e centile des écoutes. Sans données d'écoute, il n'y a pas de cible, donc pas de modèle. Elles servent aussi à calculer les deux variables d'historique d'artiste. Pour ces deux usages, des agrégats par titre suffisent, ce qui justifie l'option C.

**Attente raisonnable.** Les utilisateurs de ListenBrainz publient eux-mêmes leurs écoutes, placées sous CC0. Une réutilisation agrégée par titre prolonge cette publication. Mais la CC0 ne vaut pas consentement au sens du RGPD et ne lève aucune obligation : son § 4 exclut explicitement les droits des tiers.

**Minimisation, décidée champ par champ** d'après l'inventaire du 24/09/2026 :

| Champ | Présence | Décision |
| --- | --- | --- |
| `user_id` | 100 % | Pseudonymisé |
| `user_name` | 100 % | Supprimé avant tout stockage |
| `timestamp` | 100 % | Seconde en zone brute, jour en zone affinée |
| `submission_client`, `media_player`, `submission_client_version` | 63,6 %, 50,5 %, 0,1 % | Supprimés : informations sur l'appareil, sans lien avec la finalité |
| `tags`, `rating` | 0,1 %, 0,2 % | Supprimés : texte libre saisi par l'utilisateur |
| Titre, artiste, album, identifiants MusicBrainz, durée | 13,1 % à 100 % | Conservés |

**MusicBrainz.** Un artiste peut être une personne physique. Sont retenus l'identifiant, le nom, le type (personne, groupe…) et la zone géographique ; sont exclus le genre et les dates de naissance et de décès, dont la finalité n'a pas besoin.

## 5. Droits des personnes

**Article 14, information** : la situation du § 3.1 du document amendé s'applique sans changement, avec une notice publiée (action A5), complétée pour mentionner ListenBrainz.

**Zone brute, pendant 30 jours au plus** : l'article 11 **ne peut pas être invoqué**, puisque les lignes sont rapprochables du dump public. Les droits s'exercent de la façon suivante. La personne donne son nom ListenBrainz. Le DPO obtient le `user_id` correspondant à partir de la source publique, calcule le jeton, puis extrait ou supprime les lignes.

**Zone affinée** : un agrégat ne permet pas d'isoler une personne, et on ne peut pas retirer un élément d'une esquisse HLL. Une demande d'effacement reçue pendant les 30 jours est satisfaite en recalculant les esquisses concernées à partir de la zone brute, une fois la personne retirée. Passé ce délai, la contribution d'une personne ne peut plus être ni isolée ni retirée, et l'article 11 s'applique.

**Réserve assumée** : une esquisse de faible cardinalité (un titre écouté par une ou deux personnes dans la journée) conserve les empreintes hachées de ses éléments. La zone affinée est donc traitée comme **pseudonymisée, et non anonyme** : elle reste soumise au RGPD et bénéficie des mêmes mesures de sécurité.

## 6. Risques nouveaux ou modifiés

| Réf. | Risque | Mesure | Niveau résiduel |
| --- | --- | --- | --- |
| R2 (révisé) | Singularisation par recoupement | Aucune ligne individuelle en zone affinée ; 30 jours au plus en zone brute | Limité |
| R6 | Recoupement de la zone brute avec le dump public, sans le sel | Durée courte, mêmes contrôles d'accès que le document amendé (§ 4.3) | Limité |
| R7 | Test d'appartenance sur une esquisse de faible cardinalité | Entrées hachées avec un sel secret ; zone affinée traitée comme pseudonymisée | Négligeable |
| R8 | Réintroduction d'un champ supprimé par une évolution du schéma source | Contrôle bloquant **négatif** dans la porte de qualité : l'absence de `user_name`, `submission_client`, `media_player`, `tags` et `rating` est vérifiée, et un jeu volontairement dégradé doit le faire échouer (tâche 2.5) | Négligeable |
| R1 (aggravé) | Sel exposé dans les plans Spark | Action A2 obligatoire **avant** la première ingestion ListenBrainz, et non plus avant la mise en production | Limité |

## 7. Actions préalables à la première ingestion

| Réf. | Action | Tâche du chantier |
| --- | --- | --- |
| B1 | Créer un secret de pseudonymisation propre à ListenBrainz | 0.4 |
| B2 | Préfixe de zone brute daté : expiration à 30 jours et purge des versions non courantes à 1 jour, soit un effacement effectif à 31 jours au lieu de 60 avec la règle actuelle du bucket | 0.4 |
| B3 | Réaliser l'action A2 : sortir le sel des plans Spark | 1.1 |
| B4 | Contrôle négatif des champs supprimés (R8) | 2.5 |
| B5 | Compléter la notice d'information (A5) | Avant la mise en production |

## 8. Conclusion

Avec l'option C et les actions B1 à B3 réalisées, le niveau de risque résiduel reste **acceptable**. Sans l'option C, il ne le serait pas : conserver durablement des lignes individuelles horodatées issues d'un jeu public rendrait la pseudonymisation inopérante face à un recoupement.

| | |
| --- | --- |
| **Rédigé par** | Loïc Rabetsanta — 24 septembre 2026 |
| **Avis du DPO** | *En attente* |

## Amendement n° 1 — 24 septembre 2026

### A. Les agrégats sont quasi individuels

Mesure sur 485 194 425 écoutes (2010-08 → 2016-12), esquisses HLL fusionnées :

| Grain | Groupes | Un seul auditeur | Moins de 5 auditeurs |
| --- | --- | --- | --- |
| Jour | 347 768 169 | 89,8 % | 99,3 % |
| Semaine | 244 351 340 | 82,5 % | 97,1 % |
| Mois | 166 347 280 | 78,0 % | 94,8 % |

La phrase du § 3 « aucune ligne individuelle n'atteint la zone affinée » reste exacte au sens où aucune ligne ne porte d'identifiant ni de jeton ; elle est trompeuse si on la lit comme une garantie d'anonymat. La zone affinée est et reste une donnée **pseudonymisée**, soumise au RGPD.

### B. Seuil minimal d'auditeurs à la diffusion

Mesure ajoutée : aucun indicateur publié vers un label pour un titre écouté par moins de *k* auditeurs sur la période. Valeur de *k* à fixer après mesure de son effet. Contrôle bloquant négatif dans la porte de qualité : lignes publiées sous le seuil = 0, et échec vérifié sur un jeu volontairement dégradé (tâches 2.5 et 5.3).

### C. Rangement de la zone affinée

Grain jour, partitionnement par mois : `trois_v/ecoutes_jour/mois=AAAA-MM/`. Le rejeu d'un jour remplace ses lignes sans doublon.

| Réf. | Action | Tâche |
| --- | --- | --- |
| B6 | Fixer *k* à partir d'une mesure du nombre de titres publiables | 5.3 |
| B7 | Contrôle bloquant « aucune ligne publiée sous *k* » | 2.5 |

## Amendement n° 2 — 25 septembre 2026

**A. Minimisation par liste blanche.** La table aplatie (`listenbrainz/aplati/`, compartiment brut, expiration 30 jours) ne contient que les champs déclarés dans le schéma du job `3v_24`. Toute clé hors liste est comptée à chaque traitement et n'est jamais stockée. Les écoutes marquées `incognito_mode = true` sont exclues entièrement (16 106 écoutes), par respect de la volonté exprimée par l'utilisateur. Champs écartés : identifiants de client et d'événement tiers, URL d'origine, comportement d'écoute (durée jouée, saut, raisons de début et de fin).

**B. Écart constaté et corrigé.** Le brut contenait des adresses IP que l'ingestion n'avait pas supprimées : `ip_addr` sur 54 606 lignes (1 036 jours, 2011-08 à 2016-12), avec 12 autres clés de l'historique étendu Spotify, et `source_ip` sur 12 lignes (27/02/2015). Aucune diffusion : compartiment privé chiffré par KMS, table aplatie et zone affinée non touchées. Mesure, purge et vérification réalisées les 25/09/2026 : 0 ligne touchée après purge, aucune ligne perdue. Les versions S3 antérieures expirent sous 1 jour par la règle `lb-brut-31j`.

**C. Droits.** Le rôle d'exécution EMR reçoit une politique distincte limitée à l'écriture sous `listenbrainz/aplati/` et `listenbrainz/rebut/`. Le brut d'origine (`listenbrainz/ecoutes/`) reste en lecture seule pour Spark.

| Action | Objet | Échéance |
|---|---|---|
| B8 | Ingestion `3v_10` en liste blanche, clés hors liste comptées et non stockées | avant la tâche 3.1 |
| B9 | Vérifier l'absence de versions S3 non courantes sous `listenbrainz/ecoutes/` | 27/09/2026 |
| B10 | Comptage des clés inconnues intégré à la porte de qualité, revue de toute clé nouvelle avant stockage durable | tâche 2.5 |

## Amendement n° 3 — 26 septembre 2026

**Référentiel MusicBrainz.** Le catalogue canonique MusicBrainz (CC0 1.0, licence vérifiée dans l'archive) est ingéré comme table de référence. Il ne contient aucune donnée d'utilisateur ; il contient des noms d'artistes, dont certains désignent des personnes physiques, publiés volontairement et utilisés uniquement pour relier des titres. La table `mb_correspondance_msid` associe des identifiants de titres entre eux, sans jeton d'auditeur ni date d'écoute. Aucun risque nouveau n'est identifié ; les tables intermédiaires sous `curated/trois_v/_tmp/` (identifiants de titres et comptages) sont à supprimer après la tâche 2.5.

## Amendement n° 4 — 26 septembre 2026

**Étiquettes MusicBrainz.** Les étiquettes (`recording_tag`, `tag`) sont publiées sous licence CC BY-NC-SA 3.0 US, vérifiée dans l'archive : leur usage est limité au cadre académique de la certification, jamais à un service destiné aux labels. Elles sont isolées sous des préfixes `nc_sa/`, dans des tables `mb_nc_sa_*` qui portent la licence et l'usage en paramètres, avec le fichier `COPYING` pour l'attribution ; tout dérivé reste sous la même licence. Elles ne contiennent aucune donnée d'utilisateur : pas de colonne d'éditeur, étiquettes agrégées par enregistrement. Ce sont des textes libres : le seuil de 100 enregistrements écarte les étiquettes rares, les plus susceptibles d'être idiosyncrasiques. La correspondance `id → gid` (CC0) est réduite à ses deux colonnes utiles.

| Action | Objet | Échéance |
|---|---|---|
| B11 | Aucune table `mb_nc_sa_*` ni aucun dérivé dans un livrable ou un export destiné aux labels ; contrôle des sources avant toute diffusion | permanente |

## Amendement n° 5 — 26 septembre 2026

**Porte de qualité des nouvelles natures (tâche 2.5).** Le job `3v_30` rend bloquants, avant tout stockage durable ou toute publication, les contrôles suivants, dont l'échec a été vérifié sur un jeu volontairement dégradé. Action B4 réalisée : les champs supprimés (R8 : `user_name`, `submission_client`, `submission_client_version`, `media_player`, `tags`, `rating`) ainsi que les clés purgées sont recherchés dans chaque lot brut reçu (Q2) et la table aplatie est contrôlée en liste blanche de colonnes (Q3). Action B10 réalisée : toute clé hors contrat dans un lot reçu arrête la chaîne (Q1). Action B7 réalisée pour son mécanisme : une ligne publiée sous *k* auditeurs arrête la chaîne (Q8) ; l'application à la table publiée attend la valeur de *k* (B6, tâche 5.3). Contrôle automatique ajouté pour B11 : les tables `mb_nc_sa_*` doivent porter leurs paramètres de licence et d'usage (Q7). Aucun risque nouveau n'est identifié.

## Amendement n° 6 — 26 septembre 2026

**Ingestion en liste blanche (action B8 réalisée).** L'ingestion ListenBrainz n'écrit plus que les champs déclarés « gardés » par le contrat de schéma, à tous les niveaux de l'objet. Tout autre champ — interdit, purgé, écarté ou inconnu, y compris le texte libre `comment` et les adresses réseau — est retiré avant tout stockage ; seuls son nom et son nombre d'occurrences sont conservés au manifeste, pour revue. Les champs écartés ne sont plus stockés dans la zone brute (minimisation renforcée). Les incrémentaux quotidiens sont écrits, pseudonymisés, dans une zone de transit `listenbrainz/incrementaux/`, soumise à la même expiration à 30 jours que la zone brute. Une écoute écrite hors de cette zone pendant la mise au point a été effacée définitivement, version comprise. Aucun risque nouveau n'est identifié ; le risque R8 est réduit à la source.

## Amendement n° 7 — 26 septembre 2026

**Table des écoutes incrémentales.** Les écoutes reçues chaque jour sont aplaties dans `listenbrainz/aplati_incr/`, dans le compartiment brut : mêmes champs que la table issue du dump complet (liste blanche du contrat, identifiant d'auditeur pseudonymisé avec le même sel, vérifié par recoupement), même expiration à 30 jours (règle `lb-brut-31j`). Le rôle d'exécution EMR reçoit l'écriture sur ce seul préfixe et sur son rebut ; il reste sans droit d'écriture sur les écoutes brutes et sur la zone de transit (vérifié par simulation chemin par chemin). Le rechargement d'un jour remplace ses données sans les dupliquer. Aucun risque nouveau n'est identifié.

## Amendement n° 8 — 26 septembre 2026

**Chargement quotidien automatisé (tâche 3.3).** Les dumps incrémentaux sont ingérés chaque jour à 02:00 UTC sans intervention : un planificateur démarre une machine à états qui, dump par dump, appelle une fonction d'ingestion, contrôle la continuité, lance l'aplatissement et inscrit le résultat au registre. La fonction d'ingestion applique la même liste blanche et la même pseudonymisation que l'ingestion précédente, avec un contenu écrit identique octet pour octet (vérifié sur un dump complet) ; elle lit le sel propre à ListenBrainz dans le gestionnaire de secrets sans le journaliser, et n'accède à Internet que pour télécharger les dumps publics. Quatre rôles distincts (ingestion, pilotage, machine à états, planificateur), chacun limité à ses préfixes et actions et vérifié par simulation chemin par chemin : seul le rôle d'ingestion lit le sel ; le rôle de pilotage ne lit que le registre, les manifestes (comptages) et la sortie du traitement Spark. Nouvelle règle de qualité : une écoute dont un champ a changé de type est mise au rebut (`listenbrainz/rebut_incr/`, même expiration à 30 jours) au lieu d'être stockée dans la table, dans la limite de 0,01 % du lot et d'un seul champ ; au-delà, rien n'est écrit. Cas réel : 1 écoute sur 6 354 984. Aucun risque nouveau n'est identifié.

| Action | Objet | Échéance |
|---|---|---|
| B12 | Supprimer la fonction de sonde de sortie et les objets d'essai `_essai_lambda/` (écoutes pseudonymisées et manifestes), avec l'accord de Loïc | 28/09/2026 |

## Amendement n° 9 — 27 septembre 2026

**Analyse des données (tâches AN1 à AN6).** La jointure entre le catalogue Kaggle et les écoutes ListenBrainz, la cible reconstruite et les comparaisons de représentativité ne produisent que des comptages par titre, par année ou par genre, sans jeton, sous `curated/trois_v/analyse/an1/` (`an1_combos_resolus`, `an3_cible_par_titre`, `an2_auditeurs_par_titre`). Le décompte des auditeurs distincts par titre traite les jetons pseudonymisés de la table aplatie, dans la finalité déjà déclarée (indicateur d'auditeurs distincts par titre). Les noms d'artistes cités dans les résultats sont des noms publics d'artistes, pas des données d'auditeurs.

**Écart constaté et corrigé.** Le job `3v_42` a écrit deux tables intermédiaires contenant des jetons pseudonymisés, l'une à la ligne (titre, jeton, année), l'autre par jeton, sous `curated/trois_v/analyse/an1/_tmp_an2_*`, soit dans la zone affinée soumise à une rétention de 730 jours. Cela contrevenait à l'amendement n° 1 (aucune ligne individuelle en zone affinée) et à la durée de 31 jours des données individuelles. Aucune diffusion : compartiment privé chiffré par KMS, aucune lecture hors du job. Suppression le 27/09/2026 avec l'accord de Loïc : 416 objets, versions et marqueurs compris ; contrôle : 0 version, 0 marqueur sur le préfixe, contre-épreuve positive sur une table voisine. Règle retenue : toute table intermédiaire portant un jeton est écrite dans le compartiment brut, sous la règle d'expiration de 30 jours, jamais dans la zone affinée.

| Action | Objet | Échéance |
|---|---|---|
| B13 | Tables intermédiaires à jetons de `3v_42` supprimées de la zone affinée (416 objets, 0 restant) ; règle d'emplacement des tables intermédiaires | réalisée le 27/09/2026 |

## Amendement n° 10 — 27 septembre 2026

**Effacement effectif des versions non courantes (action B9).** L'amendement n° 2 indiquait que les versions antérieures à la purge des adresses IP « expirent sous 1 jour ». Formulation corrigée : elles deviennent **éligibles** à l'effacement 1 jour après être devenues non courantes, et S3 les supprime **en différé**, sans délai garanti. Constat du 27/09 à 00:35 UTC : 1 038 versions non courantes sous `listenbrainz/ecoutes/` (1 037 jours), contenant encore les adresses IP purgées le 25/09, les plus anciennes éligibles depuis le 26/09 à 00:00 UTC ; règle `lb-brut-31j` active, filtre et délai corrects. Aucune diffusion : compartiment privé chiffré par KMS. Si des versions subsistent au contrôle du 28/09, elles sont supprimées à la main par identifiant de version, et le délai réel est consigné.

**Dump vide.** Le traitement quotidien accepte désormais une tranche publiée sans écoute (`3v_24` v7) : aucune donnée n'est écrite, seule une entrée est ajoutée au registre. Aucun risque nouveau.

| Action | Objet | Échéance |
|---|---|---|
| B9 (révisée) | Recompter les versions non courantes sous `listenbrainz/ecoutes/` ; suppression manuelle si > 0 | 28/09/2026 au matin |
| B14 | Décider de l'arrêt de la planification quotidienne à la fin du projet : un traitement de données personnelles ne doit pas tourner sans responsable qui le surveille | 28/09/2026 |
