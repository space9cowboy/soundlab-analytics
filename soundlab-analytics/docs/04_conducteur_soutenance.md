# Conducteur de soutenance — SoundLab Analytics

**Bloc 6 · Big Data** — 21 diapositives, créneau de 20 minutes
Loïc Rabetsanta — 16 septembre 2026

---

## Pourquoi ce document existe

Les notes d'orateur du diaporama totalisent **1 761 mots**, soit **12 minutes de parole pure** à 145 mots par minute. Une restitution réelle — silences, transitions, coups d'œil aux diapositives — tourne entre 16 et 18 minutes. La marge existe, mais elle est mince, et elle se perd toujours au même endroit : sur les trois ou quatre diapositives où l'on a le plus à dire.

**Une répétition sans chronomètre ne mesure rien.** Ce conducteur donne un temps cible et un temps cumulé par diapositive, pour que tu saches à tout instant si tu es en avance ou en retard, et de combien.

Le plan vise **18:50**, ce qui laisse un peu plus d'une minute de réserve sur les vingt.

---

## Les cinq repères à mémoriser

N'apprends pas vingt et un chiffres. Apprends-en cinq.

| À la fin de… | Tu dois être à |
| --- | --- |
| Diapositive 4 — l'architecture | **3:35** |
| Diapositive 10 — les benchmarks | **9:15** |
| Diapositive 13 — la surveillance | **11:55** |
| Diapositive 17 — SHAP | **15:45** |
| Diapositive 20 — les recommandations | **18:10** |

Si tu as plus de **60 secondes de retard** à l'un de ces repères, applique la liste de coupes ci-dessous. **N'accélère pas le débit** — un débit qui s'accélère s'entend, et il signale la panique bien avant de rattraper le temps.

---

## Le conducteur

| N° | Diapositive | Cible | Cumul | Phrase d'attaque |
| --- | --- | --- | --- | --- |
| 1 | Titre — SoundLab Analytics | 40 s | **0:40** | Bonjour. Trois chiffres pour cadrer : 9,7 M d'écoutes, 1,90 $ de calcul, zéro clé permanente. |
| 2 | La question posée | 45 s | **1:25** | Un label ne peut promouvoir qu'une fraction de son catalogue. Laquelle ? |
| 3 | Trois arbitrages qui ont structuré le projet | 50 s | **2:15** | Trois choix datés, et à chaque fois l'option que j'ai écartée. |
| 4 | Architecture (pleine page) | 80 s | **3:35** | Couche brute immuable, calcul sans serveur, entrepôt en étoile. EMR n'écrit jamais dans Redshift. |
| 5 | La sécurité est dans le socle | 50 s | **4:25** | Le premier compte AWS a été perdu sur une clé exposée. La réponse est structurelle, pas procédurale. |
| 6 | Le pipeline distribué | 60 s | **5:25** | Quatre jobs. Le deuxième traite 192 fois plus de données que le premier en un temps comparable. |
| 7 | Deux incidents, deux leçons d'architecture | 55 s | **6:20** | Deux extraits du journal — ceux qui ont changé une décision. |
| 8 | Analyse d'impact — RGPD | 50 s | **7:10** | La finalité ne porte pas sur les personnes : le score est par titre. Ça exclut l'article 22. |
| 9 | L'entrepôt en étoile | 50 s | **8:00** | 192 lignes de faits pour 1 de dimension. C'est cette asymétrie qui impose le modèle. |
| 10 | Benchmarks — ce que le chronomètre ne montre pas | 75 s | **9:15** | Le temps ne bouge que d'un facteur 1,53. Le coût du plan, lui, d'un facteur 1 140. |
| 11 | L'entrepôt restitue — UNLOAD | 55 s | **10:10** | Un entrepôt qui ne fait qu'absorber n'en exploite que la moitié. |
| 12 | L'orchestration : un graphe, pas un script | 50 s | **11:00** | Une dépendance n'est pas une contrainte de ressource. |
| 13 | Surveiller, c'est anticiper la coupure | 55 s | **11:55** | Deux des trois alarmes se déclenchent AVANT la coupure. C'est ça, « proactive ». |
| 14 | La fuite de cible — diapositive pivot | 70 s | **13:05** | Mon meilleur score du projet était 0,9921. Il était faux, et voici comment je l'ai démontré. |
| 15 | L'étude d'ablation | 65 s | **14:10** | L'audio seul ne fait pas mieux que le hasard. L'historique de l'artiste ajoute treize points. |
| 16 | Trois familles, un même plafond | 40 s | **14:50** | Treize millièmes séparent le meilleur du pire. Le plafond est dans les données. |
| 17 | Interprétabilité — le signal est dans l'artiste | 55 s | **15:45** | Facteur neuf entre les variables d'artiste et la meilleure variable audio. |
| 18 | Bilan des objectifs | 40 s | **16:25** | L'objectif de 0,80 n'est pas atteint — et c'est l'objectif qui était faux. |
| 19 | Six incidents, une seule leçon | 60 s | **17:25** | Aucun n'était une panne. Tous étaient des défaillances de la vérification. |
| 20 | Recommandations et trajectoire | 45 s | **18:10** | Le levier n'est plus algorithmique. Il est dans la donnée et dans le positionnement produit. |
| 21 | Clôture | 40 s | **18:50** | Trois choses à retenir, et je réponds à vos questions. |

---

## Si tu es en retard : l'ordre des coupes

Coupe dans cet ordre, en t'arrêtant dès que tu es revenu dans les temps. Chaque coupe est choisie pour ne rien retirer d'évaluable.

1. **Diapositive 16 — trois familles, un même plafond.** Une phrase suffit : « trois familles d'algorithmes convergent à treize millièmes près, le plafond est dans les données ». Gain : 25 s.
2. **Diapositive 7 — deux incidents.** N'en raconte qu'un, le second. Gain : 25 s.
3. **Diapositive 12 — l'orchestration.** Garde la distinction dépendance / contrainte de ressource, abandonne le détail des latences. Gain : 20 s.
4. **Diapositive 8 — RGPD.** Garde les deux premiers arguments, abandonne le mécanisme de l'article 11.2. Gain : 20 s.
5. **Diapositive 3 — arbitrages.** Deux arbitrages au lieu de trois. Gain : 20 s.

**Ce qu'on ne coupe jamais** : l'architecture (4), les benchmarks (10), la fuite (14), les six incidents (19). Ce sont les quatre diapositives qui portent les compétences évaluées et la seule chose que le jury ne retrouvera pas ailleurs.

---

## Les six questions à préparer

Ce sont celles que le rendu appelle. Prépare une réponse de trois phrases pour chacune — pas plus.

**1. Pourquoi pas Hadoop ni Databricks, alors que le brief les nomme ?**
HDFS résout la localité des données, ce qui suppose de garder les machines allumées pour garder les données. Le pipeline tourne huit minutes par jour : le taux d'utilisation serait de 0,6 %. Quant à Databricks, ses capacités qui font la valeur — notebooks collaboratifs, Delta Lake, Unity Catalog — n'ont aucun destinataire ici : un seul opérateur, un seul écrivain par table.

**2. Votre optimisation Redshift ne gagne que 1,53 fois. Est-ce que ça valait le coup ?**
Au chronomètre, non, pas encore. Au plan d'exécution, le coût estimé passe de 202 millions à 178 mille, soit un facteur 1 140, parce que le plan est structurellement différent : plus aucune donnée ne circule à la jointure. À 9,7 millions de lignes sur 8 RPU, l'optimisation est une assurance sur le passage à l'échelle, pas un gain immédiat. Et je le dis comme ça dans le rapport.

**3. Vous annoncez 48 % de stockage en plus. C'est un échec ?**
C'est un arbitrage, et il est chiffré : agrégation 1,53 fois plus rapide contre stockage 1,48 fois plus lourd, pour environ un centime et demi par mois. Les deux causes sont mesurées — un facteur d'asymétrie de 253 sur la clé de distribution, et un condensat cryptographique incompressible par construction. Au milliard de lignes, l'arbitrage devrait être réexaminé, et c'est la recommandation R4.

**4. Votre objectif d'AUC n'est pas atteint.**
Non, et c'est l'objectif qui était faux. Il avait été calibré sur un prototype à 0,9921 dont l'étude d'ablation établit qu'il reconstruisait la variable cible à partir des données d'engagement. Trois familles d'algorithmes et huit configurations convergent entre 0,774 et 0,788 : le plafond est dans les données. J'ai proposé une mesure de remplacement, le gain sur une sélection aléatoire, qui vaut 1,76.

**5. Vous dites que le succès dépend de l'artiste. Comment le savez-vous ?**
Trois méthodes indépendantes convergent : l'ablation montre que l'audio seul ne dépasse pas le hasard et que l'historique d'artiste ajoute treize points ; les corrélations le confirment ; les valeurs de Shapley le quantifient, avec un facteur neuf. Attention à la formulation : le modèle **s'appuie** sur l'historique de l'artiste. Il ne démontre pas que cet historique **cause** le succès — un facteur commun peut expliquer les deux.

**6. Qu'est-ce qui manque à votre dispositif ?**
La détection de dérive des distributions. Mes contrôles vérifient la conformité structurelle à chaque exécution, pas l'évolution dans le temps. Concrètement, ma surveillance sait dire qu'un job a échoué ; elle ne sait pas dire qu'il a réussi sur des données devenues fausses. C'est la première extension que je recommande, et elle est écrite comme telle dans le rapport.

---

## Les trois pièges de langage

Ces trois formulations te coûteraient plus cher que n'importe quelle lacune technique, parce qu'elles suggèrent que tu n'as pas compris ton propre résultat.

| Ne dis pas | Dis |
| --- | --- |
| « J'obtiens 0,795 d'AUC » | « 0,7875 plus ou moins 0,0059 » — le 0,795 vient d'une séparation unique, utile pour tracer des courbes, pas pour annoncer un résultat |
| « L'artiste cause le succès » | « Le modèle s'appuie principalement sur l'historique de l'artiste » |
| « L'optimisation améliore tout » | « Elle accélère l'agrégation de 1,53 fois et alourdit le stockage de 48 % » |

---

## Protocole de répétition

Trois passages, pas un.

**Passage 1 — à voix haute, chronomètre visible, sans t'arrêter.** Même si tu te trompes, tu continues. Note le temps atteint à chacun des cinq repères. C'est une mesure, pas une performance.

**Passage 2 — corrige uniquement les écarts.** Ne réécris rien d'autre. Si tu es à 11:30 au repère de la diapositive 10 au lieu de 9:15, le problème est dans les diapositives 1 à 10, pas dans les suivantes.

**Passage 3 — complet, sans notes sous les yeux.** Si tu ne peux pas enchaîner une diapositive sans lire sa note, c'est que cette diapositive porte une idée que tu n'as pas encore faite tienne — et c'est celle sur laquelle le jury posera sa question.

**Un signe qui ne trompe pas** : si tu finis en avance au passage 3 alors que tu finissais en retard au passage 1, tu n'as pas accéléré, tu as arrêté de chercher tes mots.
