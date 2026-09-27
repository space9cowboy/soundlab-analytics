-- =============================================================================
--  SoundLab Analytics — Bloc 6 Big Data
--  Entrepôt Redshift Serverless : schéma en étoile et chargement
--
--  Ce fichier crée DEUX versions de chaque table :
--    · une version optimisée   (clés de distribution et de tri, encodages)
--    · une version naïve       (DISTSTYLE EVEN, aucun tri, aucune compression)
--
--  La version naïve n'a pas vocation à servir : elle constitue le point de
--  comparaison des benchmarks. Une recommandation d'optimisation sans mesure
--  avant/après n'est qu'une opinion.
--
--  Placeholders substitués à l'exécution par infra/rs.sh :
--    {{ROLE_ARN}}    rôle IAM assumé par Redshift pour lire S3
--    {{BUCKET_CUR}}  bucket de la couche curated
-- =============================================================================

-- @ Création du schéma
CREATE SCHEMA IF NOT EXISTS soundlab;

-- =============================================================================
--  DIMENSION — dim_track
--
--  50 683 lignes, environ 7 Mio. Une dimension de cette taille appelle
--  DISTSTYLE ALL : la table est répliquée sur chaque tranche de calcul, si
--  bien qu'une jointure avec la table de faits n'entraîne aucune
--  redistribution réseau. Le coût est le stockage multiplié par le nombre de
--  tranches — négligeable à cette échelle, décisif à l'échelle du million de
--  lignes, où DISTKEY redeviendrait le bon choix.
--
--  SORTKEY (track_id) alimente les zone maps : Redshift mémorise la plage de
--  valeurs de chaque bloc d'un mébioctet et saute les blocs hors périmètre
--  sans les lire.
--
--  L'ORDRE DES COLONNES REPRODUIT EXACTEMENT CELUI DU FICHIER PARQUET.
--  COPY FORMAT AS PARQUET associe les colonnes PAR POSITION : un ordre
--  différent chargerait des données décalées sans lever d'erreur.
-- =============================================================================

-- @ Suppression des tables existantes
DROP TABLE IF EXISTS soundlab.fact_listening CASCADE;

-- @ Suppression dim_track
DROP TABLE IF EXISTS soundlab.dim_track CASCADE;

-- @ Suppression des tables temoins
DROP TABLE IF EXISTS soundlab.fact_listening_naif CASCADE;

-- @ Suppression dim_track_naif
DROP TABLE IF EXISTS soundlab.dim_track_naif CASCADE;

-- @ Creation de dim_track (optimisee)
CREATE TABLE soundlab.dim_track (
    track_id             VARCHAR(64)      NOT NULL ENCODE ZSTD,
    name                 VARCHAR(2000)             ENCODE ZSTD,
    artist               VARCHAR(1000)             ENCODE ZSTD,
    spotify_preview_url  VARCHAR(1000)             ENCODE ZSTD,
    spotify_id           VARCHAR(128)              ENCODE ZSTD,
    tags                 VARCHAR(8000)             ENCODE ZSTD,
    genre                VARCHAR(128)              ENCODE ZSTD,
    year                 INTEGER                   ENCODE AZ64,
    duration_ms          INTEGER                   ENCODE AZ64,
    danceability         DOUBLE PRECISION          ENCODE ZSTD,
    energy               DOUBLE PRECISION          ENCODE ZSTD,
    key                  INTEGER                   ENCODE AZ64,
    loudness             DOUBLE PRECISION          ENCODE ZSTD,
    mode                 INTEGER                   ENCODE AZ64,
    speechiness          DOUBLE PRECISION          ENCODE ZSTD,
    acousticness         DOUBLE PRECISION          ENCODE ZSTD,
    instrumentalness     DOUBLE PRECISION          ENCODE ZSTD,
    liveness             DOUBLE PRECISION          ENCODE ZSTD,
    valence              DOUBLE PRECISION          ENCODE ZSTD,
    tempo                DOUBLE PRECISION          ENCODE ZSTD,
    time_signature       INTEGER                   ENCODE AZ64,
    duration_min         DOUBLE PRECISION          ENCODE ZSTD,
    nb_tags              INTEGER                   ENCODE AZ64,
    date_ingestion       TIMESTAMP                 ENCODE AZ64
)
DISTSTYLE ALL
COMPOUND SORTKEY (track_id);

-- =============================================================================
--  TABLE DE FAITS — fact_listening
--
--  9 711 301 lignes. Deux décisions structurent ses performances.
--
--  DISTKEY (track_id) : les lignes d'une même piste atterrissent sur la même
--  tranche. La requête métier centrale — agréger les écoutes par piste — se
--  calcule alors localement, sans redistribution. Avec DISTSTYLE EVEN, chaque
--  GROUP BY track_id déclencherait un brassage complet des 9,7 M de lignes.
--
--  Le choix de la clé de distribution suit la clé de jointure et de
--  regroupement, pas la colonne la plus cardinale. track_id compte 30 459
--  valeurs distinctes, largement de quoi répartir sans point chaud.
--
--  COMPOUND SORTKEY (track_id) : classe physiquement les lignes par piste, ce
--  qui rend l'agrégation séquentielle et permet aux zone maps d'éliminer les
--  blocs non concernés lors d'un filtre sur track_id.
--
--  Encodages : AZ64 sur les entiers et horodatages (algorithme propriétaire
--  Redshift, plus efficace que ZSTD sur les types numériques), ZSTD sur les
--  chaînes. user_id_hash est un CHAR(32) de longueur fixe — un condensat
--  tronqué à 128 bits, donc incompressible par construction ; l'encodage n'y
--  gagne presque rien, mais le type de longueur fixe évite le surcoût de
--  l'en-tête de longueur des VARCHAR.
--
--  ORDRE DES COLONNES : track_id, playcount, user_id_hash, date_ingestion.
--  C'est celui du Parquet, où user_id_hash occupe la troisième position parce
--  que le job de la tâche 8 l'ajoute en fin de DataFrame avant de supprimer
--  user_id qui occupait la première.
-- =============================================================================

-- @ Creation de fact_listening (optimisee)
CREATE TABLE soundlab.fact_listening (
    track_id        VARCHAR(64)  NOT NULL ENCODE ZSTD,
    playcount       INTEGER      NOT NULL ENCODE AZ64,
    user_id_hash    CHAR(32)     NOT NULL ENCODE ZSTD,
    date_ingestion  TIMESTAMP             ENCODE AZ64
)
DISTKEY (track_id)
COMPOUND SORTKEY (track_id);

-- =============================================================================
--  TABLES TÉMOINS — le point de comparaison
--
--  Mêmes données, aucune optimisation : distribution en tourniquet, aucun
--  tri, aucune compression. C'est la configuration qu'on obtient en laissant
--  Redshift tout décider sur une table créée à la va-vite.
-- =============================================================================

-- @ Creation de dim_track_naif (temoin)
CREATE TABLE soundlab.dim_track_naif (
    track_id             VARCHAR(64) ENCODE RAW,
    name                 VARCHAR(2000) ENCODE RAW,
    artist               VARCHAR(1000) ENCODE RAW,
    spotify_preview_url  VARCHAR(1000) ENCODE RAW,
    spotify_id           VARCHAR(128) ENCODE RAW,
    tags                 VARCHAR(8000) ENCODE RAW,
    genre                VARCHAR(128) ENCODE RAW,
    year                 INTEGER ENCODE RAW,
    duration_ms          INTEGER ENCODE RAW,
    danceability         DOUBLE PRECISION ENCODE RAW,
    energy               DOUBLE PRECISION ENCODE RAW,
    key                  INTEGER ENCODE RAW,
    loudness             DOUBLE PRECISION ENCODE RAW,
    mode                 INTEGER ENCODE RAW,
    speechiness          DOUBLE PRECISION ENCODE RAW,
    acousticness         DOUBLE PRECISION ENCODE RAW,
    instrumentalness     DOUBLE PRECISION ENCODE RAW,
    liveness             DOUBLE PRECISION ENCODE RAW,
    valence              DOUBLE PRECISION ENCODE RAW,
    tempo                DOUBLE PRECISION ENCODE RAW,
    time_signature       INTEGER ENCODE RAW,
    duration_min         DOUBLE PRECISION ENCODE RAW,
    nb_tags              INTEGER ENCODE RAW,
    date_ingestion       TIMESTAMP ENCODE RAW
)
DISTSTYLE EVEN;

-- @ Creation de fact_listening_naif (temoin)
CREATE TABLE soundlab.fact_listening_naif (
    track_id        VARCHAR(64) ENCODE RAW,
    playcount       INTEGER     ENCODE RAW,
    user_id_hash    CHAR(32)    ENCODE RAW,
    date_ingestion  TIMESTAMP   ENCODE RAW
)
DISTSTYLE EVEN;

-- =============================================================================
--  CHARGEMENT
--
--  COPY lit directement le Parquet depuis S3, en parallèle sur toutes les
--  tranches. C'est la méthode de chargement massif de Redshift : un INSERT
--  ligne à ligne serait plusieurs ordres de grandeur plus lent.
--
--  Le rôle IAM porte kms:Decrypt sur la clé du projet — le bucket curated est
--  chiffré par une CMK, et sans ce droit COPY échoue sur une erreur d'accès
--  qui ne mentionne jamais KMS.
-- =============================================================================

-- @ COPY dim_track
COPY soundlab.dim_track
FROM 's3://{{BUCKET_CUR}}/music_info/'
IAM_ROLE '{{ROLE_ARN}}'
FORMAT AS PARQUET;

-- @ COPY dim_track_naif
COPY soundlab.dim_track_naif
FROM 's3://{{BUCKET_CUR}}/music_info/'
IAM_ROLE '{{ROLE_ARN}}'
FORMAT AS PARQUET;

-- @ COPY fact_listening (9,7 M lignes)
COPY soundlab.fact_listening
FROM 's3://{{BUCKET_CUR}}/listening_history/'
IAM_ROLE '{{ROLE_ARN}}'
FORMAT AS PARQUET;

-- @ COPY fact_listening_naif (9,7 M lignes)
COPY soundlab.fact_listening_naif
FROM 's3://{{BUCKET_CUR}}/listening_history/'
IAM_ROLE '{{ROLE_ARN}}'
FORMAT AS PARQUET;

-- =============================================================================
--  STATISTIQUES
--
--  ANALYZE met à jour les statistiques de distribution des colonnes, dont le
--  planificateur se sert pour estimer la cardinalité des jointures et choisir
--  entre hash join et merge join. Sans statistiques à jour, un plan peut
--  partir sur une estimation fausse de plusieurs ordres de grandeur.
-- =============================================================================

-- @ ANALYZE dim_track
ANALYZE soundlab.dim_track;

-- @ ANALYZE fact_listening
ANALYZE soundlab.fact_listening;

-- @ ANALYZE dim_track_naif
ANALYZE soundlab.dim_track_naif;

-- @ ANALYZE fact_listening_naif
ANALYZE soundlab.fact_listening_naif;

-- =============================================================================
--  CONTRÔLES DE CHARGEMENT
-- =============================================================================

-- @ Verification des volumetries
SELECT 'dim_track' AS table_chargee, COUNT(*) AS lignes FROM soundlab.dim_track
UNION ALL SELECT 'fact_listening', COUNT(*) FROM soundlab.fact_listening
UNION ALL SELECT 'dim_track_naif', COUNT(*) FROM soundlab.dim_track_naif
UNION ALL SELECT 'fact_listening_naif', COUNT(*) FROM soundlab.fact_listening_naif
ORDER BY 1;

-- @ Controle d'integrite du chargement
SELECT
    COUNT(DISTINCT f.track_id)     AS pistes_ecoutees,
    COUNT(DISTINCT f.user_id_hash) AS auditeurs,
    MIN(f.playcount)               AS playcount_min,
    MAX(f.playcount)               AS playcount_max
FROM soundlab.fact_listening f;
