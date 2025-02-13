# ATL-Datamart Version Python
==============================

Projet pour :  
* Cours d'atelier Architecture décisionnelle Datamart (TRDE704) pour les I1 de l'EPSI Paris et Arras.

Le sujet est à disposition dans le dossier `docs` et à jour dans votre espace learning.

## Comment utiliser ce template ?

### Gérer l'infrastructure
Vous n'avez pas besoin d'installer l'ensemble de l'architecture sur votre PC. L'intégralité de l'architecture (à l'exception des dépendances de développement) est gérée par le fichier de configuration `docker-compose.yml`.

* Pour lancer l'infrastructure
  ```sh
  docker compose up
  ```
* Pour stopper l'infrastructure
  ```sh
  docker compose down
  ```

**Remarque Linux** :  
- En cas de problème avec le daemon de Docker, c'est parce que vous avez deux Docker installés sur votre machine (Docker Desktop et Docker Engine via CLI). Seule la version Engine sera active, et donc uniquement accessible via `sudo`. Si vous avez besoin de gérer visuellement les conteneurs, je vous invite à utiliser [Portainer](https://docs.portainer.io/start/install-ce/server).

### Description détaillée du sujet

* Pour le TP 1 :  
  * Il faudra utiliser le fichier situé à `src/data/grab_parquet.py` et compléter les fonctions qui sont vides.  
  * Remarque : Ne vous cassez pas la tête à effacer les fonctions. Sinon, vous allez augmenter exponentiellement la difficulté de réaliser ce TP.
  
* Pour le TP 2 : Il existe deux approches :  
  Le but de ce TP est de récupérer les fichiers **Parquet** stockés dans votre datalake (qui est Minio) pour les stocker en l'état **brut**, vers le Data Warehouse (ici PostgreSQL par défaut).
  * Soit vous utilisez [Amphi.AI](https://amphi.ai/), qui est un ETL open source (difficulté élevée) (***SVP, faites-moi un retour régulier pour l'améliorer !***)
  * Soit vous adaptez le code situé dans `src/data/dump_to_sql.py`. La version actuelle récupère les fichiers stockés en local et déverse les données dans le Data Warehouse sans les optimisations d'injection. Votre but ici sera d'adapter le code pour récupérer depuis le datalake Minio, et d'optimiser la vitesse de l'injection des données.
     * **Remarque** : L'implémentation actuelle permet uniquement de prendre les fichiers **Parquet** sauvegardés en local. Vous devriez modifier le programme pour qu'il récupère les fichiers **Parquet** que vous avez stockés dans Minio.
     
* Pour le TP 3 :  
  * Vous devez utiliser des requêtes SQL sur le SGBD de votre choix afin de créer les tables selon le modèle en flocon. Par souci de simplicité du sujet, vous êtes libre d'utiliser le SGBD de votre choix sans tenir compte des propriétés OLAP.
  * Vous aurez donc un script SQL pour chaque tâche distincte :
    * `creation.sql` pour la création des tables en flocon avec les contraintes associées.
    * `insertion.sql` pour insérer les données depuis votre base de données `Data Warehouse` vers votre base de données `Data Mart`.  
       * Remarque : Ce sont bien **DEUX SERVEURS SGBD** distincts **ET NON DEUX BASES DE DONNÉES DANS UN MÊME SERVEUR** !
       
* Pour le TP 4 :  
  * Lorsque vous aurez fait le TP 3, vous devriez normalement avoir une idée sur la restitution des données que vous souhaitez faire dans la partie Dataviz.
    * Si ce n'est pas le cas, vous pouvez ouvrir un Notebook qui sera sauvegardé dans le dossier `notebooks` pour réaliser votre Analyse Exploratoire de Données (EDA).
    * Pour les plus chauds d'entre vous, vous pouvez concevoir un tableau de bord à l'aide de [Streamlit](https://streamlit.io/), vous y trouverez des exemples dans la section Gallery.
  * Vous devez connecter votre outil de Data Visualisation à votre base de **données** `Data Mart` afin de produire les visualisations.
  
* Pour le TP 5 :  
  * Cette partie du TP vous servira d'introduction à l'orchestration des tâches d'un projet Big Data. C'est-à-dire de lancer des scripts Python de manière totalement automatisée sur un **intervalle** défini.
  * Pour le moment, je vous demande de réaliser un **DAG** qui permet de télécharger un fichier **Parquet** du dernier mois en vigueur et de le stocker dans Minio.
  * Une fois que vous aurez compris le fonctionnement des **DAGs**, vous pouvez vous amuser à automatiser le TP 2 et le TP 3 afin de rendre le TP 4 totalement autonome.

Pour le TP 5, il faudra créer vous-même le répertoire suivant :  
Sinon, vous risquez d'avoir des problèmes lors du lancement des conteneurs.

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
