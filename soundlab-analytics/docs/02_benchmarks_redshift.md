# Analyse des performances de l'entrepôt Redshift

**Projet de certification Bloc 6 — Big Data**
SoundLab Analytics · Loïc Rabetsanta

---

## 1. Objet

Mesurer l'effet réel des choix de modélisation physique de l'entrepôt, et non les postuler. Chaque scénario est exécuté sur deux jeux de tables portant **exactement les mêmes données** :

| | Tables retenues | Tables témoins |
|---|---|---|
| Distribution de la dimension | `DISTSTYLE ALL` | `DISTSTYLE EVEN` |
| Distribution des faits | `DISTKEY (track_id)` | `DISTSTYLE EVEN` |
| Tri | `COMPOUND SORTKEY (track_id)` | aucun |
| Encodage | ZSTD sur les chaînes, AZ64 sur les entiers | `RAW` partout |

Les tables témoins n'ont aucune vocation opérationnelle. Elles existent pour qu'une recommandation d'optimisation repose sur une mesure plutôt que sur une opinion.

## 2. Environnement

| | |
|---|---|
| Service | Amazon Redshift Serverless, version 1.0.416217 |
| Capacité de base | 8 RPU (minimum régional en `eu-north-1`) |
| Région | `eu-north-1` (Stockholm) |
| Accès | Redshift Data API, authentification par identité IAM |
| `dim_track` | 50 683 lignes, 24 colonnes |
| `fact_listening` | 9 711 301 lignes, 4 colonnes |

## 3. Protocole

Trois précautions rendent les mesures exploitables.

**Cache de résultats désactivé.** Redshift renvoie en quelques millisecondes le résultat d'une requête déjà vue. Sans `SET enable_result_cache_for_session TO off`, le benchmark mesurerait le cache et non la modélisation.

**Session persistante.** Ce réglage ne vaut que pour la session courante, or la Redshift Data API ouvre par défaut une session distincte à chaque instruction. Le pilote `infra/rs.sh` maintient donc une session unique sur toute la durée du fichier — sans quoi la désactivation du cache serait perdue dès la requête suivante.

**Une passe de chauffe avant chaque mesure.** Redshift compile les segments de requête à la première exécution et met le code compilé en cache. La première passe mesure donc la compilation autant que l'exécution.

### 3.1 Un défaut du protocole, et sa correction

Le protocole prévoyait de faire passer le témoin **avant** la version optimisée dans chaque scénario, afin qu'un éventuel réchauffement du cache disque profite au témoin plutôt qu'à la configuration que l'on cherche à valoriser.

Cette précaution s'est retournée contre elle-même : la toute première requête de la session a absorbé le démarrage à froid du calcul, affichant **26 799 ms**. La preuve de l'artefact est immédiate — la chauffe de la version optimisée exécutée juste après tombe à 621 ms, et la mesure du témoin ensuite à 608 ms.

**Ce chiffre de 26,8 s ne mesure pas l'effet de `DISTSTYLE EVEN` et n'est pas retenu comme résultat.** La correction consiste à exécuter une requête jetable avant tous les scénarios, pour absorber le démarrage à froid hors de toute mesure.

### 3.2 Seuil de significativité

Redshift Serverless ajuste sa capacité en cours d'exécution. Un écart inférieur à 20 % n'est pas interprété ; seuls les facteurs 2 et au-delà, ou les différences structurelles de plan, sont considérés comme démontrés.

## 4. Résultats

| Scénario | Témoin | Optimisé | Écart | Conclusion |
|---|---|---|---|---|
| **A** — agrégation par piste | 608 ms | 397 ms | × 1,53 | Gain réel |
| **B** — jointure faits × dimension | 255 ms | 238 ms | × 1,07 | Non significatif |
| **C** — filtre de plage sur la clé de tri | 90 ms | 79 ms | × 1,14 | Non significatif |
| **D** — `dim_track` sur disque | 3 456 blocs | 110 blocs | × 31 | Gain majeur |
| **D** — `fact_listening` sur disque | 1 280 blocs | 1 890 blocs | × 0,68 | **Surcoût** |
| **E** — coût estimé de la jointure | 202 909 982 | 177 982 | × 1 140 | Plan structurellement différent |

## 5. Analyse par scénario

### 5.1 Scénario A — l'agrégation par piste

C'est la requête métier centrale du projet : elle produit `total_plays` et `unique_listeners`, les deux variables dont dépend le modèle.

```sql
SELECT track_id, SUM(playcount), COUNT(DISTINCT user_id_hash)
FROM soundlab.fact_listening GROUP BY track_id ORDER BY 2 DESC LIMIT 10;
```

Avec `DISTKEY (track_id)`, toutes les lignes d'une même piste résident sur la même tranche de calcul : le regroupement se fait localement. Le témoin, distribué en tourniquet, doit redistribuer les 9,7 millions de lignes sur le réseau avant de pouvoir regrouper.

**Gain mesuré : facteur 1,53.** Réel, au-dessus du seuil de significativité, mais loin d'un ordre de grandeur.

### 5.2 Scénarios B et C — aucune différence mesurable

Sept et douze pour cent d'écart : du bruit. Deux explications, toutes deux vérifiables.

Pour **B**, huit RPU traitent 9,7 millions de lignes sans effort, et diffuser une dimension de 50 000 lignes vers chaque tranche coûte peu. La redistribution que `DISTSTYLE ALL` évite existe bien, mais son coût est absorbé.

Pour **C**, le filtre `track_id BETWEEN 'TRA' AND 'TRC'` retient 883 844 lignes, soit 9 % du volume. Ce n'est pas assez sélectif pour que l'élagage de blocs par les *zone maps* se traduise en temps mesurable. Un filtre portant sur quelques centaines de lignes donnerait un résultat très différent.

**Ces deux non-résultats sont consignés tels quels.** Un benchmark qui ne rapporte que ses succès n'est pas un benchmark.

### 5.3 Scénario E — la preuve que l'optimisation existe

Là où le chronomètre ne montre rien, le planificateur est explicite.

**Témoin** — coût estimé 202 909 982 :

```
XN HashAggregate
  ->  XN Hash Join DS_BCAST_INNER
        ->  XN Seq Scan on fact_listening_naif
        ->  XN Hash
              ->  XN Seq Scan on dim_track_naif
```

**Optimisé** — coût estimé 177 982 :

```
XN HashAggregate
  ->  XN Hash Join DS_DIST_ALL_NONE
        ->  XN Seq Scan on fact_listening
        ->  XN Hash
              ->  XN Seq Scan on dim_track
```

`DS_BCAST_INNER` signifie que la table interne est **diffusée vers toutes les tranches** avant la jointure. `DS_DIST_ALL_NONE` signifie qu'**aucune donnée ne circule** : la dimension est déjà présente partout.

Le rapport des coûts estimés est de **1 140**. Le plan est qualitativement différent ; c'est seulement à cette échelle de données que la différence ne se voit pas encore au chronomètre.

> **L'optimisation est une assurance sur le passage à l'échelle, et le plan d'exécution la démontre même quand le temps ne la montre pas encore.**

### 5.4 Scénario D, première découverte — le gaspillage de blocs

`dim_track_naif` occupe **3 456 blocs d'un mébioctet pour environ 7 Mio de données utiles** : près de cinq cents fois sa taille réelle.

Le chiffre s'explique exactement : 3 456 = 24 × 144, soit une colonne multipliée par le nombre de tranches de calcul. **Redshift alloue un bloc d'un mébioctet par colonne et par tranche, même quasi vide.** Une petite table large distribuée en tourniquet ne stocke presque que du vide.

`dim_track`, en `DISTSTYLE ALL` et avec encodage, occupe **110 blocs** — un facteur 31.

C'est un argument supplémentaire, et contre-intuitif, en faveur de `DISTSTYLE ALL` sur les petites dimensions : on croit payer une réplication, on économise en réalité un gaspillage structurel.

### 5.5 Scénario D, seconde découverte — l'optimisation coûte du stockage

`fact_listening` optimisée occupe **1 890 blocs contre 1 280** pour le témoin, soit **48 % de plus**. L'encodage ZSTD et AZ64 n'a pas compensé.

Deux causes se cumulent.

**L'asymétrie de distribution.** Mesurée sur les données elles-mêmes :

```sql
SELECT MAX(n), AVG(n), MAX(n)/AVG(n)
FROM (SELECT track_id, COUNT(*) AS n FROM soundlab.fact_listening GROUP BY track_id);
```

| Lignes pour la piste la plus écoutée | Moyenne par piste | Facteur d'asymétrie |
|---|---|---|
| 80 656 | 318 | **253** |

Avec `DISTKEY (track_id)`, ces 80 656 lignes atterrissent toutes sur **une seule tranche**. Les tranches se remplissent très inégalement, et chacune arrondit au bloc d'un mébioctet supérieur.

**Un condensat ne se compresse pas.** `user_id_hash` est un SHA-256 tronqué à 128 bits — indistinguable du hasard par construction. Ni ZSTD ni le dictionnaire n'ont de prise. Or cette colonne représente l'essentiel du volume de la table.

### 5.6 Mise en perspective du surcoût

L'écart de 610 blocs représente environ 0,6 Gio, soit de l'ordre de **1,5 centime par mois** au tarif du stockage managé. Le ratio est réel, le montant est négligeable à cette échelle.

Le rapporter reste nécessaire : à l'échelle du milliard de lignes, 48 % de stockage supplémentaire cesse d'être anecdotique, et l'arbitrage devrait être réexaminé.

## 6. Recommandations

| Réf. | Recommandation | Fondement |
|---|---|---|
| **R1** | **Conserver `DISTKEY (track_id)`** sur `fact_listening` | Gain de 1,53 × sur la requête métier centrale. Surcoût de stockage de 48 % accepté en connaissance de cause, négligeable en valeur absolue |
| **R2** | **Conserver `DISTSTYLE ALL`** sur `dim_track` | 31 × moins de blocs, et `DS_DIST_ALL_NONE` au plan. Réévaluer si la dimension dépasse le million de lignes |
| **R3** | **Précalculer une table d'agrégat** `track_engagement` (30 459 lignes, `DISTSTYLE ALL`) | La requête métier centrale deviendrait une lecture de 30 000 lignes au lieu d'une agrégation sur 9,7 millions. C'est le produit naturel des tâches 10-11 |
| **R4** | **Réexaminer la clé de distribution si le volume croît d'un ordre de grandeur** | Le facteur d'asymétrie de 253 crée des tranches surchargées. Une clé composite, ou `DISTSTYLE AUTO`, mériteraient d'être comparés |
| **R5** | **Corriger le protocole de benchmark** par une requête jetable initiale | Le démarrage à froid a faussé la première mesure (§ 3.1) |
| **R6** | **Ne pas ajouter d'index — ils n'existent pas** | Redshift n'a ni index ni clés primaires contraignantes. Les leviers sont la distribution, le tri, l'encodage et les statistiques. C'est un point que les benchmarks confirment : tout se joue à la modélisation physique |

## 7. Limites de l'étude

**Le volume est trop faible pour départager certains choix.** Neuf millions de lignes et environ deux gibioctets ne suffisent pas à saturer huit RPU. Les scénarios B et C le montrent : les différences existent au plan, pas au chronomètre. Une extrapolation honnête suppose un volume dix à cent fois supérieur.

**Une seule exécution par mesure.** Faute de répétitions, aucun intervalle de confiance ne peut être calculé. Le seuil de 20 % tient lieu de garde-fou, mais un protocole rigoureux exigerait cinq exécutions et une médiane.

**La capacité varie en cours d'exécution.** Redshift Serverless ajuste ses RPU selon la charge, ce qui introduit une variance non contrôlée que ce protocole ne peut isoler.

**Les tables témoins ne reproduisent pas un défaut réaliste.** `ENCODE RAW` partout est une configuration que personne ne choisirait volontairement — Redshift applique `ENCODE AUTO` par défaut. Le témoin représente donc le pire cas, non le cas moyen, ce qui majore mécaniquement les écarts de stockage.

## 8. Reproductibilité

```bash
source .soundlab.env
./infra/rs.sh sql/01_schema_etoile.sql    # création et chargement
./infra/rs.sh sql/02_benchmarks.sql       # mesures
```

| Fichier | Rôle |
|---|---|
| `sql/01_schema_etoile.sql` | Schéma en étoile, tables témoins, `COPY`, `ANALYZE` |
| `sql/02_benchmarks.sql` | Les cinq scénarios, avec chauffes et plans |
| `infra/rs.sh` | Pilote Data API à session persistante |
| `infra/04_redshift.sh` | Provisionnement de l'entrepôt |

Durée totale du chargement : 27,5 s de calcul Redshift, dont 3,1 s pour le `COPY` des 9,7 millions de lignes.
