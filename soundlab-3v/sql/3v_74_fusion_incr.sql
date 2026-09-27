-- =============================================================================
--  SoundLab Analytics — chantier trois V, tâche 4.2
--  Fusion incrémentale des faits ListenBrainz (à lancer après le job 3v_61 incr)
--
--    ../soundlab-analytics/infra/rs.sh sql/3v_74_fusion_incr.sql
--
--  Au lieu de recharger 520 M de lignes, on ne touche que les groupes
--  (date_jour, recording_msid, origine='incr') nouveaux ou dont les comptes ont
--  changé. Les écarts sont d'abord calculés dans stg_incr_ecarts, puis fusionnés
--  par MERGE. Rejouer la fusion sur les mêmes données ne trouve aucun écart et ne
--  modifie donc rien : la fusion est idempotente par construction, et le
--  contrôle final le vérifie.
--
--  Limite assumée : un groupe présent dans l'entrepôt mais absent de la nouvelle
--  sortie de 3v_61 n'est pas supprimé. C'est voulu : quand la règle lb-brut-31j
--  aura fait expirer les premiers dumps (vers le 25/10), 3v_61 reconstruira des
--  agrégats sur une fenêtre plus courte ; l'entrepôt, qui ne porte aucun jeton,
--  doit conserver les comptes déjà acquis. En contrepartie, un compte d'un jour
--  couvert par un dump expiré pourrait être ré-écrit à la baisse : à traiter
--  avant cette échéance si la planification reste active (B14 prévoit de la
--  désactiver après le rendu).
-- =============================================================================

-- @ Vidage de stg_incr
TRUNCATE soundlab_lb.stg_incr;

-- @ COPY stg_incr (sortie courante de 3v_61 incr)
COPY soundlab_lb.stg_incr
FROM 's3://{{BUCKET_CUR}}/trois_v/entrepot/faits_jour/origine=incr/'
IAM_ROLE '{{ROLE_ARN}}'
FORMAT AS PARQUET;

-- @ Suppression stg_incr_ecarts (rejeu)
DROP TABLE IF EXISTS soundlab_lb.stg_incr_ecarts;

-- @ Calcul des ecarts (groupes nouveaux ou modifies)
CREATE TABLE soundlab_lb.stg_incr_ecarts DISTKEY (recording_msid) AS
SELECT s.date_jour, s.recording_msid, s.ecoutes, s.auditeurs,
       CASE WHEN f.recording_msid IS NULL THEN 'nouveau' ELSE 'modifie' END AS nature
FROM soundlab_lb.stg_incr s
LEFT JOIN soundlab_lb.fait_ecoutes_jour f
       ON f.origine = 'incr' AND f.date_jour = s.date_jour AND f.recording_msid = s.recording_msid
WHERE f.recording_msid IS NULL OR f.ecoutes <> s.ecoutes OR f.auditeurs <> s.auditeurs;

-- @ Ecarts a fusionner
SELECT nature, COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes FROM soundlab_lb.stg_incr_ecarts GROUP BY nature ORDER BY 1;

-- @ MERGE des ecarts dans fait_ecoutes_jour
MERGE INTO soundlab_lb.fait_ecoutes_jour
USING soundlab_lb.stg_incr_ecarts e
ON fait_ecoutes_jour.origine = 'incr'
   AND fait_ecoutes_jour.date_jour = e.date_jour
   AND fait_ecoutes_jour.recording_msid = e.recording_msid
WHEN MATCHED THEN UPDATE SET ecoutes = e.ecoutes, auditeurs = e.auditeurs
WHEN NOT MATCHED THEN INSERT VALUES (e.date_jour, e.recording_msid, e.ecoutes, e.auditeurs, 'incr');

-- @ Controle · ecarts restants apres fusion (attendu 0)
SELECT COUNT(*) AS ecarts_restants
FROM soundlab_lb.stg_incr s
LEFT JOIN soundlab_lb.fait_ecoutes_jour f
       ON f.origine = 'incr' AND f.date_jour = s.date_jour AND f.recording_msid = s.recording_msid
WHERE f.recording_msid IS NULL OR f.ecoutes <> s.ecoutes OR f.auditeurs <> s.auditeurs;

-- @ Controle · volumetries par origine
SELECT origine, COUNT(*) AS groupes, SUM(ecoutes) AS ecoutes, MAX(date_jour) AS dmax
FROM soundlab_lb.fait_ecoutes_jour GROUP BY origine ORDER BY 1;
