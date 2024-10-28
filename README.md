ATL-Datamart Version Python
==============================

Projet pour : 
* Cours d'atelier Architecture décisionnel Datamart (TRDE704) pour les I1 de l'EPSI Paris et Arras.

Le sujet est à disposition dans le dossier docs ET le sujet à jour dans votre espace learning.

## Comment utiliser ce template ?
*  Pour le TP 1 :
    * Il faudra utiliser le fichier qui se situe à `src/data/grab_parquet.py` et compléter les fonctions qui sont vides.
    * Remarque : Ne vous cassez pas la tête à effacer les fonctions. Sinon, vous allez augmenter exponentiellement la difficulté de réaliser ce TP.
*  Pour le TP 2 :
    * Etant donnée qu'il n'existe pas d'ETL gratuit et/ou opensource traitant les fichiers parquets visuellement. Il faudra utiliser le fichier `src/data/dump_to_sql.py`.
    * L'implémentation actuel permet uniquement de prendre les fichiers parquets sauvegardés en local. Vous devriez modifier le programme pour qu'il récupère les fichiers parquets que vous avez stockés dans Minio.
*  Pour le TP 3:
    * Vous devez utiliser les requêtes SQL sur le SGBD de votre choix afin de créer les tables en modèle en Flocon. Par soucis de simplicité du sujet, vous êtes libre utiliser le SGBD de votre choix sans tenir compte des propriété OLAP.
    * Vous aurez donc un script SQL pour chaque tâche distinct :
      * `creation.sql` pour la création des tables en flocons avec les contraintes associés.
      * `insertion.sql` pour insérer les données depuis votre base de donnée `Data Warehouse` vers votre base de donnée `Data Mart`.
         * Remarque : C'est bien **DEUX SERVEURS SGBD** distinct **ET NON DEUX BASES DE DONNEES** !
*   Pour le TP 4 :
      * Lorsque vous avez fait le TP3, vous devriez normalement avoir une idée sur la restitution des données que vous souhaitez faire dans la partie Dataviz.
        * Si ce n'est pas le cas, vous pouvez ouvrir un Notebook qui sera sauvegardé dans le dossier `notesbooks` pour réaliser votre Analyse Exploratoire de Données (EDA).
        * Pour les élèves de DC PARIS : Vous avez le choix entre une visualisation sous MATPLOTLIB/PLOTLY/SEABORN ou bien Tableau Desktop / PowerBI.
        * Pour les plus chaud d'entre vous, vous pouvez concevoir un tableau de bord à l'aide de Streamlit.
      * Vous devez connecter votre outil de Data Visualisation à votre base de donnée `Data Mart` afin de produire les visualisations.
*   Pour le TP 5:
      * Cette partie du TP vous servira d'introduction à l'orchestration des tâches d'un projet Big Data. C'est-à-dire de lancer des scripts python de manière totalement automatisée sur un interval définie.
      * Pour le moment, je vous demande de réaliser une dag qui permet de télécharger un parquet du dernier mois en vigueur (TP 1) et de le stocker vers Minio.
      * Une fois que vous avez compris le fonctionnement des dags, vous pouvez vous amuser à automatiser le TP 2 et 3 afin de rendre le TP 4 totalement autonome.

Pour le TP 5, il faudra créer vous-même le répertoire suivant :
Sinon vous risquez d'avoir des problèmes au lancement des conteneurs.
------------

    ├── airflow
    │   ├── config       <- Configuration files related to the Airflow Instance
    │   ├── dags         <- Folder that contains all the dags
    │   ├── logs         <- Contains the logs of the previously dags run
    │   └── plugins      <- Should be empty : Contains all needed plugins to make the dag work
   


--------

Project Organization
------------
    ├── airflow
    │   ├── config       <- Configuration files related to the Airflow Instance
    │   ├── dags         <- Folder that contains all the dags
    │   ├── logs         <- Contains the logs of the previously dags run
    │   └── plugins      <- Should be empty : Contains all needed plugins to make the dag work
    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
