"""
DAG de verification de l'installation - SoundLab Analytics.

Il ne touche a rien : quatre commandes shell qui affichent du texte.
Son seul role est de prouver que le planificateur lit les fichiers, que les
taches s'executent et que la forme du graphe est bien celle qu'on a declaree.

Il reproduit volontairement la forme du DAG reel, qui est un losange et non
une chaine :

    source_a ---+
                +--> porte_qualite --> traitement_aval
    source_b ---+

source_a et source_b ne dependent pas l'une de l'autre. C'est porte_qualite
qui a besoin des deux.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ARGUMENTS_PAR_DEFAUT = {
    # Une seule nouvelle tentative : sur un echec de logique metier, reessayer
    # ne sert a rien. Le reessai est utile contre un alea reseau, pas contre
    # une erreur de code.
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "depends_on_past": False,
}

with DAG(
    dag_id="00_verification_installation",
    description="Verification de l'installation - aucune dependance AWS",
    default_args=ARGUMENTS_PAR_DEFAUT,
    start_date=datetime(2026, 9, 1),
    # None : le DAG ne se declenche que manuellement. Les donnees du projet
    # n'ont aucun horodatage, donc aucune periodicite n'aurait de sens ici.
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["soundlab", "verification"],
) as dag:

    source_a = BashOperator(
        task_id="source_a",
        bash_command="echo 'Lecture de la source A (music_info)' && sleep 3",
    )

    source_b = BashOperator(
        task_id="source_b",
        bash_command="echo 'Lecture de la source B (listening_history)' && sleep 3",
    )

    porte_qualite = BashOperator(
        task_id="porte_qualite",
        bash_command=(
            "echo 'Controles de qualite : 13/13 passes' && "
            "echo 'Code de sortie 0 -> le graphe continue' && exit 0"
        ),
    )

    traitement_aval = BashOperator(
        task_id="traitement_aval",
        bash_command="echo 'Construction des variables, puis chargement'",
    )

    # La declaration des dependances. La liste a gauche signifie que les deux
    # taches doivent etre terminees avant que porte_qualite ne demarre.
    [source_a, source_b] >> porte_qualite >> traitement_aval
