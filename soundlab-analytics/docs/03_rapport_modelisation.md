# Modélisation prédictive — construction, ablation et évaluation

**Projet de certification Bloc 6 — Big Data**
SoundLab Analytics · Loïc Rabetsanta · 11 septembre 2026
Tâches 10 à 13 · Couvre les indicateurs RNCP 5.5 (IA éthique, explicabilité) et 6.2-6.4 (qualité du code et de la documentation)

---

## Résumé

L'objectif initial — AUC-ROC supérieure à 0,80 — **n'est pas atteint** par un modèle exempt de fuite de cible : la meilleure configuration plafonne à **0,7875 ± 0,0059** en validation croisée.

Ce n'est pas un échec de mise en œuvre. L'objectif avait été calibré sur un prototype atteignant 0,9921, dont l'étude d'ablation établit qu'il **reconstruisait la variable cible** à partir des données d'engagement. Une exigence fixée sur une mesure contaminée n'est pas une référence valide.

Trois familles d'algorithmes et huit configurations d'hyperparamètres convergent entre 0,774 et 0,788 : **le plafond est dans les données, pas dans le choix du modèle**.

L'objectif de rappel supérieur à 0,75 est, lui, atteint — au seuil de décision 0,33, avec une précision conservée de 0,4398 pour un taux de hits de 25 % dans la population, soit un **gain de 1,76 fois sur une sélection aléatoire**.

Le résultat le plus exploitable n'est pas métrique mais métier : **le succès d'un titre se prédit par son artiste, non par son contenu sonore**. Les deux variables d'historique d'artiste pèsent neuf fois plus que la meilleure caractéristique acoustique.

---

## 1. Construction du jeu de données (tâche 10)

### 1.1 Jointure et périmètre

| | |
|---|---|
| Catalogue de métadonnées | 50 683 titres |
| Historique d'écoute | 9 711 301 lignes, 962 037 auditeurs pseudonymisés |
| **Après jointure interne** | **30 459 titres · 6 207 artistes** |
| Couverture du catalogue | 60,1 % |

**Biais de sélection assumé.** La jointure interne écarte 20 224 titres n'ayant reçu aucune écoute. Le modèle ne répond donc pas à « ce titre sera-t-il un succès » mais à « **parmi les titres déjà écoutés au moins une fois, lequel atteindra le quartile supérieur** ». Cette restriction limite la portée commerciale : l'outil ne peut pas évaluer un inédit sans aucun signal.

### 1.2 Variable cible

`is_hit = 1` si `total_plays ≥ 592`, seuil correspondant au 75ᵉ percentile calculé exactement sur les 30 459 titres retenus.

Répartition : **7 617 hits · 22 842 niches**, soit 25,01 %.

Ce seuil de 592 reproduit au titre près celui du prototype développé en Google Colab, ce qui **valide le pipeline distribué de bout en bout**.

### 1.3 Taxonomie des variables

Chaque variable a été classée selon son degré d'exposition à la cible. Ce rangement est inscrit dans le code du job, pas seulement dans ce document.

| Groupe | Variables | Statut |
|---|---|---|
| **A · Audio** | 13 caractéristiques Spotify | Aucune fuite — mesurées sur le signal |
| **B · Contexte** | ancienneté, durée, taille du catalogue de l'artiste | Aucune fuite |
| **C · Artiste** | taux de hits et écoutes moyennes des **autres** titres | Aucune fuite — calcul excluant la piste courante |
| **D · Notoriété** | `nb_tags` | Indicateur indirect — les étiquettes s'accumulent avec la popularité |
| **E · Engagement** | `unique_listeners`, `avg_plays_par_auditeur`, `ratio_engagement`, `max_playcount` | **Fuite directe** |
| **F · Cible** | `total_plays`, `log_total_plays`, `is_hit` | Jamais en entrée |

### 1.4 Variables d'artiste — construction

Pour chaque titre, on calcule la performance des **autres** titres du même artiste, en retirant la contribution de la ligne courante à l'agrégat du groupe.

Les 2 600 titres (8,54 %) dont l'artiste n'a qu'une œuvre au catalogue reçoivent `NULL` et non zéro : **l'information est absente, pas médiocre**. Imputer par la moyenne ou par zéro inventerait un signal.

Ces variables sont légitimes en production — l'historique d'un artiste est connu avant la sortie d'un nouveau titre. Mais elles dérivent des cibles d'autres lignes, ce qui impose une contrainte d'évaluation traitée au § 3.1.

### 1.5 Normalisation — non appliquée, et pourquoi

Aucune transformation d'échelle n'est appliquée à la table de features. Deux raisons distinctes.

**Une forêt aléatoire est invariante à l'échelle.** Un arbre découpe sur des seuils ; multiplier une colonne par mille ne change aucune décision. Normaliser avant un modèle à base d'arbres est un calcul sans effet.

**Calibrer un scaler sur l'ensemble des données serait une seconde fuite**, plus discrète : les moyennes et écarts-types du jeu de test se retrouveraient dans les paramètres appliqués à l'entraînement. Un scaler se calibre sur le jeu d'entraînement seul.

Les statistiques descriptives complètes sont néanmoins persistées comme artefact, pour tout modèle sensible à l'échelle. Le § 4.2 montre comment cette contrainte est respectée par construction via un `Pipeline`.

---

## 2. La fuite de cible — identification et mesure

### 2.1 Le soupçon

Le prototype affichait **0,9921** d'AUC-ROC. Un score aussi élevé sur un problème réputé difficile est un indice, pas une prouesse.

`is_hit` se définit par `total_plays ≥ 592`. Or `total_plays = SUM(playcount)` et `unique_listeners = COUNT(*)` sont calculés **sur les mêmes lignes** de l'historique. Avec un `playcount` moyen de 2,631, le seuil de 592 écoutes équivaut approximativement à un seuil de **225 auditeurs**.

### 2.2 Les corrélations, et leur piège

| Mesure | Valeur |
|---|---|
| `corr(unique_listeners, total_plays)` | 0,9116 |
| `corr(unique_listeners, log_total_plays)` | 0,4575 |
| `corr(unique_listeners, is_hit)` | 0,4078 |

Le coefficient de 0,91 est **gonflé par les valeurs extrêmes** : un titre totalise 527 893 écoutes pour 80 656 auditeurs, contre 318 lignes par titre en moyenne. Après passage au logarithme, la relation retombe à 0,46.

Ces chiffres suggéraient que `unique_listeners` n'était **pas** la cible déguisée. **C'était une erreur d'instrument** : le coefficient de Pearson mesure une relation linéaire, alors que la relation est monotone mais fortement non linéaire. Une forêt aléatoire, qui découpe sur des seuils, l'exploite sans difficulté — tandis que l'AUC, fondée sur le classement, ne dépend pas de la linéarité.

**Une analyse de corrélation est insuffisante pour établir une fuite.** Seule l'ablation tranche.

### 2.3 L'identité algébrique

Un second mécanisme subsiste, exact et non statistique :

```
avg_plays_par_auditeur = total_plays / unique_listeners
⟹  unique_listeners × avg_plays_par_auditeur = total_plays     exactement
```

Prises isolément, ces deux variables corrèlent faiblement avec la cible — 0,41 et 0,11. **Ensemble, elles la reconstituent parfaitement.**

Conséquence méthodologique : **une analyse variable par variable est aveugle à ce type de fuite.** Le groupe d'engagement doit être écarté **en bloc**, non variable par variable.

---

## 3. Étude d'ablation (tâche 12)

### 3.1 Protocole

**Séparation groupée par artiste.** Les variables d'artiste étant calculées sur les autres titres du même artiste, une séparation aléatoire ferait passer les étiquettes du jeu de test dans les variables d'entraînement. `GroupShuffleSplit` puis `GroupKFold` sur `artist` garantissent qu'un artiste est entièrement d'un côté ou de l'autre.

**Valeurs manquantes traitées comme une information.** Sentinelle hors domaine accompagnée d'un indicateur binaire explicite, laissant l'arbre isoler les observations sans historique plutôt que de les confondre avec des observations moyennes.

**Modèle constant** sur toutes les variantes : forêt aléatoire, 300 arbres, `min_samples_leaf=5`, `class_weight="balanced_subsample"`, graine 42.

### 3.2 Résultats

Séparation unique, 20 % de test.

| Variante | Variables | AUC-ROC | Précision au rappel 0,75 |
|---|---|---|---|
| **A** — audio seul | 13 | 0,5979 | 0,3018 |
| **B** — + contexte | 16 | 0,6585 | 0,3364 |
| **C** — + artiste | 18 | **0,7953** | **0,4736** |
| **D** — + notoriété | 19 | 0,7979 | 0,4691 |
| *E* — audio + `unique_listeners` | 14 | *0,9896* | *0,9800* |
| *F* — audio + engagement complet | 17 | *0,9997* | *1,0000* |

*Les variantes E et F ne sont pas des candidates : elles servent à établir l'ampleur de la fuite.*

### 3.3 Lecture

**La variante A reproduit le prototype V2** (0,5979 contre 0,5959) : le signal acoustique seul ne dépasse pas significativement le hasard.

**La variante E reproduit le prototype V1** (0,9896 contre 0,9921), et `unique_listeners` y concentre **0,880 de l'importance**. L'énigme est résolue : cette seule variable produisait le score. La fuite est établie.

**La variante F atteint 0,9997**, ce qui confirme la reconstruction algébrique du § 2.3.

**La progression A → B → C est le résultat exploitable** : 0,598 → 0,659 → 0,795. L'audio ne vaut guère mieux que le hasard ; le contexte ajoute six points ; **l'historique de l'artiste en ajoute treize**.

**La variante D est écartée.** L'écart de 0,0026 avec C est très inférieur au bruit, et `nb_tags` est un indicateur indirect de popularité au statut ambigu. Testée, sans apport mesurable, retirée.

### 3.4 Validation croisée

Cinq plis groupés par artiste.

| Variante | AUC moyen | Écart-type | Plis |
|---|---|---|---|
| **C** | **0,7875** | 0,0059 | 0,797 · 0,788 · 0,782 · 0,781 · 0,790 |
| D | 0,7902 | 0,0064 | 0,801 · 0,787 · 0,784 · 0,785 · 0,794 |

La séparation unique donnait 0,7953, soit **la valeur la plus haute de tous les plis** : elle était optimiste de sept millièmes. Même à deux écarts-types, la borne haute atteint 0,799 — **l'objectif de 0,80 n'est pas dans l'intervalle**.

### 3.5 Mesure de la fuite liée à la séparation

Même variante, même modèle, même graine — seule la méthode de séparation change.

| Séparation | AUC |
|---|---|
| Groupée par artiste | 0,7953 |
| Aléatoire | 0,8069 |
| **Écart** | **+0,0116** |

Une séparation aléatoire surestime la performance d'un point d'AUC. **Seule la valeur groupée est publiable.**

---

## 4. Sélection de modèle (tâche 12b)

### 4.1 Comparaison

Trois familles d'algorithmes, variante C, mêmes plis.

| Modèle | AUC moyen | σ | Précision au rappel 0,75 | Gain sur hasard |
|---|---|---|---|---|
| Régression logistique | 0,7743 | 0,0079 | 0,4290 | × 1,72 |
| **Forêt aléatoire** | **0,7875** | 0,0058 | **0,4398** | **× 1,76** |
| Boosting par histogramme | 0,7814 | 0,0034 | 0,4306 | × 1,72 |
| Boosting, meilleure des 8 configurations | 0,7855 | 0,0057 | 0,4317 | × 1,73 |

### 4.2 Trois stratégies de valeurs manquantes

Chaque algorithme traite différemment les 8,54 % de titres sans historique d'artiste, ce qui fait de cette comparaison une évaluation implicite des stratégies d'imputation :

- **Régression logistique** — médiane et indicateur, calculés **dans un `Pipeline`**, donc recalibrés sur chaque pli d'entraînement seulement. C'est la mise en œuvre concrète du principe énoncé au § 1.5 : la garantie est structurelle, pas déclarative.
- **Forêt aléatoire** — sentinelle hors domaine et indicateur.
- **Boosting** — valeurs manquantes gérées nativement, l'arbre apprenant de quel côté les envoyer.

Aucune stratégie ne se détache : l'écart entre les trois modèles reste inférieur à 1,4 point.

### 4.3 Conclusion

**Treize millièmes séparent le meilleur du pire.** Trois familles d'algorithmes et huit jeux d'hyperparamètres atterrissent entre 0,774 et 0,788.

Le fait le plus informatif : **la régression logistique est à 1,3 point de la forêt aléatoire**. Si les données recelaient des interactions riches, les modèles à base d'arbres l'écraseraient. Ils ne le font pas — il n'y a presque pas de structure non linéaire exploitable.

**Le plafond est dans les données, pas dans l'algorithme.**

Précision d'honnêteté : l'écart entre la forêt aléatoire et le meilleur boosting est de 0,002, très en deçà du bruit. **Le choix de la forêt n'est pas statistiquement fondé** — il se justifie par la continuité avec le prototype et par la lisibilité de ses importances de variables.

---

## 5. Évaluation du modèle retenu (tâche 13)

### 5.1 Protocole

Le modèle publié en production est réentraîné sur l'intégralité des données, ce qui **interdit toute évaluation** : aucune observation ne lui est inconnue. L'évaluation reproduit donc la séparation de l'ablation — `GroupShuffleSplit`, graine 42, 20 % de test, regroupement par artiste — et porte exclusivement sur 6 359 titres jamais vus.

### 5.2 Métriques

| Métrique | Valeur |
|---|---|
| AUC-ROC | 0,7953 |
| **AUC précision-rappel** | **0,5561** |
| Taux de hits du jeu de test | 0,2595 |
| Seuil d'exploitation retenu | 0,3675 |
| Rappel à ce seuil | 0,7503 |
| Précision à ce seuil | 0,4736 |
| Gain sur une sélection aléatoire | × 1,83 |

**L'AUC précision-rappel est la métrique la plus honnête ici.** Sur des classes déséquilibrées, la courbe ROC flatte : elle récompense la bonne détection des négatifs, majoritaires et faciles. Un rapport de 0,556 à 0,260 — soit **2,14 fois le taux de base** — mesure l'apport réel.

> **Distinction à maintenir dans tout le rendu.** Les chiffres de cette section proviennent d'**une** séparation, nécessaire pour tracer des courbes. Les chiffres à annoncer sont ceux de la validation croisée : **AUC 0,7875 ± 0,0059** et précision au rappel visé **0,4398**, soit un gain de **1,76**. Annoncer 0,795 en s'appuyant sur les courbes puis 0,787 ailleurs serait incohérent.

### 5.3 Le rappel est un choix d'exploitation

Le rappel n'est pas une propriété du modèle mais du seuil de décision. Au seuil par défaut de 0,5, le rappel vaut 0,578 ; en l'abaissant à 0,33, il atteint l'objectif de 0,75. **La grandeur à arbitrer est la précision conservée à ce niveau de rappel.**

Traduction opérationnelle : sur un catalogue de 6 000 titres dont environ 1 500 perceront, l'outil en signale à peu près 2 400, parmi lesquels se trouvent 1 125 des 1 500 succès. Un label qui concentre son budget sur ces 2 400 titres **divise son effort par 2,5 en ne perdant qu'un quart des succès**.

---

## 6. Interprétabilité (tâche 13)

### 6.1 Pourquoi SHAP plutôt que les importances

`feature_importances_` mesure une réduction moyenne d'impureté : grandeur globale, positive par construction, biaisée en faveur des variables à forte cardinalité. Elle établit qu'une variable compte, jamais comment ni dans quel sens.

SHAP attribue à **chaque prédiction** une contribution signée par variable, et ces contributions s'additionnent exactement pour reconstituer l'écart entre la prédiction et la moyenne. Cette propriété d'additivité rend la décomposition auditable.

Valeurs calculées sur un échantillon de 3 000 observations du jeu de test, tirage à graine fixe.

### 6.2 Classement

| Variable | \|SHAP\| moyen | Sens de l'effet |
|---|---|---|
| Taux de hits des autres titres de l'artiste | **0,1091** | valeur élevée → hit |
| Écoutes moyennes des autres titres | **0,0832** | valeur élevée → hit |
| Caractère instrumental | 0,0210 | valeur élevée → **niche** |
| Dansabilité | 0,0170 | valeur élevée → hit |
| Nombre d'autres titres au catalogue | 0,0163 | valeur élevée → hit |

### 6.3 Lecture

**Les deux variables d'artiste totalisent 0,192 contre 0,021 pour la meilleure caractéristique audio — un facteur neuf.** Le signal est dans l'artiste, non dans le son. Trois méthodes indépendantes convergent : corrélations, ablation, SHAP.

**La répartition est saine.** Aucune variable n'écrase les autres comme le faisait `unique_listeners` avec ses 0,880 d'importance dans la variante E. Le modèle combine réellement plusieurs sources d'information — c'est la vérification que l'épisode de la fuite rendait nécessaire.

**Les deux effets audio sont musicalement cohérents.** Un titre instrumental perce moins ; un titre dansant perce davantage. Cette vraisemblance est un contrôle utile : un effet inversé aurait signalé une erreur de traitement.

### 6.4 Ce que SHAP n'établit pas

**SHAP explique le modèle, pas le monde.** Le fait que l'historique de l'artiste domine signifie que **le modèle s'appuie sur cette information**, non que cet historique cause le succès. Un facteur commun peut expliquer les deux : puissance du label, réseau de diffusion, budget promotionnel.

Formulation juste : « le modèle s'appuie principalement sur l'historique de l'artiste ». Formulation fautive : « l'historique de l'artiste cause le succès ».

---

## 7. Bilan des objectifs

| Objectif | État | Valeur |
|---|---|---|
| AUC-ROC > 0,80 | ❌ **Non atteint** | 0,7875 ± 0,0059 |
| Rappel > 0,75 | ✅ Atteint | 0,7503 au seuil 0,33 |

**L'objectif de 0,80 n'était pas une référence valide.** Il avait été calibré sur un prototype à 0,9921 dont l'ablation démontre qu'il reconstruisait la cible. Ce rapport ne constate pas un échec : il établit qu'une exigence fixée sur une mesure contaminée devait être révisée, et propose la mesure honnête à lui substituer.

**Objectif de remplacement proposé** : précision au rappel de 0,75 supérieure à deux fois le taux de base. Valeur obtenue : **1,76**. Objectif non atteint mais proche, et mesuré sans complaisance.

---

## 8. Recommandations

| Réf. | Recommandation | Fondement |
|---|---|---|
| **M1** | Réviser l'objectif de performance et l'exprimer en gain sur sélection aléatoire | L'AUC est peu parlante pour un label ; le gain de 1,76 se traduit directement en budget |
| **M2** | Repositionner le produit sur la trajectoire d'artiste plutôt que sur l'analyse du morceau | Les variables d'artiste pèsent neuf fois plus que l'audio |
| **M3** | Ne pas chercher de gain algorithmique supplémentaire | Trois familles convergent à 0,013 près : le plafond est dans les données |
| **M4** | Enrichir les données plutôt que le modèle — signaux éditoriaux, playlists, budget promotionnel | C'est la seule voie de progression identifiée |
| **M5** | Acquérir des données horodatées pour permettre une évaluation temporelle | Lèverait la fuite structurelle et autoriserait une vraie prédiction |
| **M6** | Conserver les variantes E et F dans le dépôt, documentées comme témoins | Elles matérialisent la fuite et protègent d'une régression future |

---

## 9. Limites

**Aucun horodatage dans les données.** La correction canonique d'une fuite de cible est temporelle : mesurer les auditeurs sur une première fenêtre, les écoutes sur une seconde, prédire la seconde à partir de la première. Le jeu de données ne le permet pas.

**Biais de sélection à 60,1 %.** Le modèle n'est valide que sur des titres ayant déjà reçu au moins une écoute.

**Une seule graine aléatoire.** La validation croisée donne une dispersion entre plis, mais pas la variabilité liée à l'initialisation. Un protocole plus rigoureux répéterait sur plusieurs graines.

**Variables d'artiste dérivées des cibles d'autres lignes.** Le regroupement par artiste neutralise la fuite en évaluation, mais l'effet de bord subsiste pour les artistes à catalogue très réduit.

**SHAP calculé sur 3 000 observations** et non sur la totalité du jeu de test, pour des raisons de coût. Suffisant pour des moyennes globales, insuffisant pour une analyse de sous-populations rares.

---

## 10. Reproductibilité

```bash
source .soundlab.env
source ~/.venvs/soundlab/bin/activate

# Tâches 10-11 — construction du jeu de données (PySpark sur EMR Serverless)
./infra/submit_job.sh jobs/10_feature_engineering.py …

# Tâche 12 — ablation en six variantes
python ml/12_entrainement_ablation.py …

# Tâche 12b — sélection de modèle
python ml/13_comparaison_modeles.py …

# Tâche 13 — évaluation et SHAP
python ml/14_shap_evaluation.py …
```

| Fichier | Rôle |
|---|---|
| `jobs/10_feature_engineering.py` | Jointure, cible, variables d'artiste, diagnostic de fuite |
| `ml/12_entrainement_ablation.py` | Six variantes, validation croisée, diagnostic de séparation |
| `ml/13_comparaison_modeles.py` | Trois algorithmes, réglage, publication du modèle |
| `ml/14_shap_evaluation.py` | Courbes d'évaluation, valeurs SHAP, comparaison des classements |
| `ml/requirements.txt` | Environnement figé |
| `mlruns/mlflow.db` | Suivi MLflow des expériences |

**Graine fixée à 42** dans l'ensemble de la chaîne. **Modèle publié** dans `s3://soundlab-models-558852/modele_final/modele_soundlab.joblib`, avec ses variables, son seuil recommandé et son AUC de validation croisée.

### Note d'architecture

Les tâches 10-11 s'exécutent en PySpark sur EMR Serverless : 9,7 millions de lignes à agréger. Les tâches 12-13 s'exécutent **localement** : 30 459 lignes ne justifient plus un cluster, et provisionner des exécuteurs pour entraîner sur 30 000 observations coûterait plus que le calcul lui-même.

**Spark là où le volume le justifie, la pile ML standard là où il ne le justifie plus.** C'est un arbitrage, pas une facilité.
