-- =============================================================================
--  SoundLab Analytics — Tâche 16 : restitution de l'entrepôt vers le lac
--
--  CE QUE FAIT CE FICHIER
--  Il construit dans l'entrepôt un référentiel d'artistes — nombre de titres,
--  écoutes cumulées, écoutes médianes, taux de hits — par jointure en étoile
--  entre les 50 683 lignes de `dim_track` et les 9 711 301 lignes de
--  `fact_listening`, puis l'exporte en Parquet vers la couche curated.
--
--  POURQUOI CET AGRÉGAT-LÀ
--  L'analyse SHAP de la tâche 12 a montré que le signal prédictif se
--  concentre sur l'artiste, pas sur les descripteurs audio. L'entrepôt
--  produit donc le référentiel d'artistes : c'est l'objet métier que le
--  modèle a désigné comme central.
--
--  POURQUOI L'ENTREPÔT ET PAS SPARK
--  C'est exactement la forme de calcul pour laquelle un entrepôt colonnaire
--  est conçu : agrégation sur une clé de faible cardinalité, jointure sur une
--  DISTKEY commune, aucune donnée à sortir du cluster. Le même calcul en
--  Spark impliquerait de relire les Parquet depuis S3 et de payer un shuffle.
--
--  POURQUOI UNE VUE ET PAS UNE REQUÊTE DANS LE SCRIPT
--  La définition du hit (592 écoutes cumulées) est une règle métier. Si elle
--  vit dans un script d'export, elle se dédouble : le modèle a la sienne,
--  l'entrepôt la sienne, et elles divergent sans que personne s'en aperçoive.
--  Placée dans une vue, elle est déclarée une fois, dans l'entrepôt, et le
--  contrôle A vérifie qu'elle coïncide toujours avec celle du modèle.
--
--  ATTENTION — CLEANPATH
--  L'option CLEANPATH vide la destination avant d'écrire. C'est pourquoi
--  l'export vise un sous-préfixe dédié `exports_entrepot/referentiel_artistes/`
--  et non la racine `exports_entrepot/` : un CLEANPATH sur la racine
--  effacerait tout autre export qui y serait déposé plus tard.
--
--  Prérequis : bash infra/07_droits_unload.sh  (droit d'écriture sur le préfixe)
--  Usage     : ./infra/rs.sh sql/03_unload_referentiel.sql
-- =============================================================================


-- @ Controle A — population de la jointure et seuil de hit
--
--  Deux vérifications en une seule requête :
--   1. la population jointe. Le modèle a été entraîné sur les pistes ayant au
--      moins une écoute, soit 60,1 % du catalogue (biais de sélection assumé,
--      documenté en tâche 9). L'entrepôt doit retrouver la même population.
--   2. le seuil. Spark l'a obtenu par approxQuantile(0.75, erreur relative 0),
--      qui renvoie une valeur réellement présente dans les données.
--      PERCENTILE_DISC a la même sémantique et doit donc donner 592.
--      PERCENTILE_CONT interpole entre deux valeurs voisines : un écart de sa
--      part est normal et sans conséquence, il est affiché pour mémoire.
WITH engagement AS (
    SELECT track_id, SUM(playcount)::BIGINT AS total_plays
    FROM soundlab.fact_listening
    GROUP BY track_id
),
pistes AS (
    SELECT e.total_plays
    FROM soundlab.dim_track d
    JOIN engagement e ON e.track_id = d.track_id
),
seuils AS (
    SELECT
        PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY total_plays) OVER () AS p75_disc,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_plays) OVER () AS p75_cont
    FROM pistes
)
SELECT
    (SELECT COUNT(*) FROM soundlab.dim_track)                       AS catalogue,
    (SELECT COUNT(*) FROM pistes)                                   AS pistes_ecoutees,
    ROUND(100.0 * (SELECT COUNT(*) FROM pistes)
                / (SELECT COUNT(*) FROM soundlab.dim_track), 2)     AS couverture_pct,
    MAX(p75_disc)::BIGINT                                           AS seuil_entrepot_disc,
    ROUND(MAX(p75_cont), 1)                                         AS seuil_entrepot_cont,
    592                                                             AS seuil_spark
FROM seuils;


-- @ Suppression de la vue precedente
--
--  Le seuil 592 est écrit en clair, volontairement : c'est la valeur produite
--  par la tâche 10 et figée avec le modèle. Le recalculer ici ferait dériver
--  la définition du hit dès qu'une écoute est ajoutée, et le référentiel ne
--  décrirait plus le même objet que le modèle.
--  Le genre principal d'un artiste est celui de son titre le plus écouté
--  PARMI CEUX QUI EN DÉCLARENT UN. Redshift n'a pas de fonction de mode, et
--  prendre un genre au hasard (MAX alphabétique) serait une valeur arbitraire
--  présentée comme un fait.
--  La mention IGNORE NULLS n'est pas un détail : la source ne renseigne le
--  genre que pour 44,1 % des pistes. Sans elle, 2 937 artistes sortaient sans
--  genre ; avec elle il en reste 1 959, ceux qui n'en déclarent réellement
--  aucun. Les 978 autres avaient l'information ailleurs dans leur catalogue.
--  Contrepartie assumée : pour un artiste dont les titres les plus écoutés
--  sont tous muets, le genre retenu peut provenir d'un titre marginal. C'est
--  pourquoi la vue expose `nb_titres_avec_genre` — le lecteur voit sur quelle
--  assise repose la valeur au lieu de devoir la supposer.
--  DROP puis CREATE, et non CREATE OR REPLACE : Redshift n'accepte le
--  remplacement que si la vue produit exactement les mêmes colonnes, or on en
--  ajoute une. Le IF EXISTS rend le fichier rejouable.
DROP VIEW IF EXISTS soundlab.v_referentiel_artistes;


-- @ Vue soundlab.v_referentiel_artistes
CREATE VIEW soundlab.v_referentiel_artistes AS
WITH engagement AS (
    SELECT track_id,
           SUM(playcount)::BIGINT       AS total_plays,
           COUNT(DISTINCT user_id_hash) AS auditeurs_uniques
    FROM soundlab.fact_listening
    GROUP BY track_id
),
pistes AS (
    SELECT d.artist,
           -- La source melange absence de genre et chaine vide. NULLIF ramene
           -- les deux au meme cas, sans quoi IGNORE NULLS retiendrait une
           -- chaine vide comme un genre valide.
           NULLIF(TRIM(d.genre), '') AS genre,
           d.year,
           e.total_plays, e.auditeurs_uniques,
           CASE WHEN e.total_plays >= 592 THEN 1 ELSE 0 END AS is_hit
    FROM soundlab.dim_track d
    JOIN engagement e ON e.track_id = d.track_id
    WHERE d.artist IS NOT NULL
      AND LENGTH(TRIM(d.artist)) > 0
),
enrichies AS (
    SELECT p.*,
           MEDIAN(p.total_plays) OVER (PARTITION BY p.artist) AS med_plays,
           FIRST_VALUE(p.genre IGNORE NULLS) OVER (
               PARTITION BY p.artist ORDER BY p.total_plays DESC
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           ) AS genre_principal
    FROM pistes p
)
SELECT
    artist                                              AS artiste,
    COUNT(*)                                            AS nb_titres,
    SUM(total_plays)::BIGINT                            AS ecoutes_totales,
    MAX(med_plays)::BIGINT                              AS ecoutes_medianes,
    SUM(is_hit)                                         AS nb_hits,
    ROUND(100.0 * SUM(is_hit) / COUNT(*), 2)            AS taux_hits_pct,
    SUM(auditeurs_uniques)::BIGINT                      AS auditeurs_cumules,
    MIN(year)                                           AS annee_premiere,
    MAX(year)                                           AS annee_derniere,
    SUM(CASE WHEN genre IS NOT NULL THEN 1 ELSE 0 END)  AS nb_titres_avec_genre,
    MAX(genre_principal)                                AS genre_principal
FROM enrichies
GROUP BY artist;


-- @ Documentation de la vue
COMMENT ON VIEW soundlab.v_referentiel_artistes IS
'Referentiel d''artistes — agregat de dim_track x fact_listening. Un hit est un titre a 592 ecoutes cumulees ou plus, seuil P75 fige avec le modele de la tache 10. auditeurs_cumules est une somme par titre, pas un decompte d''auditeurs distincts de l''artiste. genre_principal est le genre du titre le plus ecoute qui en declare un ; nb_titres_avec_genre dit sur combien de titres cette valeur repose, et vaut 0 lorsque le genre est NULL.';


-- @ Apercu — 15 artistes les plus ecoutes
SELECT artiste, nb_titres, ecoutes_totales, ecoutes_medianes,
       nb_hits, taux_hits_pct, nb_titres_avec_genre, genre_principal
FROM soundlab.v_referentiel_artistes
ORDER BY ecoutes_totales DESC
LIMIT 15;


-- @ Export UNLOAD vers curated/exports_entrepot/referentiel_artistes/
--
--  PARALLEL OFF : sans cette option Redshift écrit un fichier par tranche de
--  calcul. Quelques milliers de lignes donneraient des dizaines de fichiers
--  quasi vides — la même erreur de granularité que la table `dim_track_naif`
--  du benchmark, transposée au stockage objet. Un seul fichier trié est ici
--  à la fois plus petit et plus rapide à relire.
--  ORDER BY : avec PARALLEL OFF le tri est global, le fichier est donc
--  directement exploitable sans tri côté lecteur.
UNLOAD ('SELECT artiste, nb_titres, ecoutes_totales, ecoutes_medianes, nb_hits, taux_hits_pct, auditeurs_cumules, annee_premiere, annee_derniere, nb_titres_avec_genre, genre_principal FROM soundlab.v_referentiel_artistes ORDER BY ecoutes_totales DESC')
TO 's3://{{BUCKET_CUR}}/exports_entrepot/referentiel_artistes/'
IAM_ROLE '{{ROLE_ARN}}'
FORMAT AS PARQUET
PARALLEL OFF
CLEANPATH;


-- @ Controle B — volumetrie du referentiel
--
--  Le nombre d'artistes et le nombre de titres couverts doivent correspondre
--  a la population du controle A, aux titres sans artiste pres.
--  `sans_genre` est le residu irreductible : des artistes dont AUCUN titre ne
--  declare de genre. Il doit valoir 1 959. S'il vaut 2 937, la mention
--  IGNORE NULLS n'a pas ete prise en compte et la vue n'a pas ete remplacee.
SELECT COUNT(*)                       AS nb_artistes,
       SUM(nb_titres)                 AS titres_couverts,
       SUM(ecoutes_totales)           AS ecoutes_totales,
       SUM(nb_hits)                   AS hits,
       ROUND(100.0 * SUM(nb_hits) / SUM(nb_titres), 2) AS taux_hits_global_pct,
       SUM(CASE WHEN genre_principal IS NULL THEN 1 ELSE 0 END) AS sans_genre,
       MAX(nb_titres)                 AS max_titres_par_artiste
FROM soundlab.v_referentiel_artistes;
