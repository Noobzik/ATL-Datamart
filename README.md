# ATL-Datamart

Atelier de Big Data : Introduction aux pipelines de données et Architecture de donnes pour DC Paris

## Comment utiliser ce template ?

-   Pour le TP 2 :
    -   Etant donnée qu'il n'existe pas d'ETL traitant les fichiers parquets visuellement. Il faudra utiliser le fichier `src/data/dump_to_sql.py`.
    -   L'implémentation actuellement permet uniquement de prendre les fichiers parquets sauvegardés en local. Vous devriez modifier le programme pour qu'il prenne les fichiers parquets que vous avez stockés dans Minio.
-   Pour le TP 3:
    -   Vous devez utiliser les requêtes SQL sur le SGBD de votre choix afin de créer les tables en modèle en Flocon. Par soucis de simplicité du sujet, vous êtes libre utiliser le SGBD de votre choix sans tenir compte des propriété OLAP.
    -   Vous aurez donc un script SQL pour chaque tâche distinct :
        -   `creation.sql` pour la création des tables en flocons avec les contraintes associés.
        -   `insertion.sql` pour insérer les données depuis votre base de donnée `Data Warehouse` vers votre base de donnée `Data Mart`
-   Pour le TP 4 :
    -   Lorsque vous avez fait le TP3, vous devriez normalement avoir une idée sur la restitution des données que vous souhaitez faire dans la partie Dataviz.
        -   Si ce n'est pas le cas, vous pouvez ouvrir un Notebook qui sera sauvegardé dans le dossier `notesbooks` pour réaliser votre Exploration Data Analysis (EDA).
        -   Pour les élèves de DC PARIS : Vous avez le choix entre une visualisation sous MATPLOTLIB/PLOTLY ou bien Tableau Desktop / PowerBI
    -   Vous devez connecter votre outil de Data Visualisation à votre base de donnée `Data Mart` afin de produire les visualisations.
-   Pour le TP 5:
    -   Cette partie du TP vous servira d'introduction à l'orchestration des tâches d'un projet Big Data. C'est-à-dire de lancer des scripts python de manière totalement automatisée sur un interval définie.
    -   Pour le moment, je vous demande de réaliser une dag qui permet de télécharger un parquet du dernier mois en vigueur (TP 1) et de le stocker vers Minio.
    -   Une fois que vous avez compris le fonctionnement des dags, vous pouvez vous amuser à automatiser le TP 2 et 3 afin de rendre le TP 4 totalement autonome.

Pour le TP 5, il faudra créer vous-même le répertoire suivant :
Sinon vous risquez d'avoir des problèmes au lancement des conteneurs.

---

    ├── airflow
    │   ├── config       <- Configuration files related to the Airflow Instance
    │   ├── dags         <- Folder that contains all the dags
    │   ├── logs         <- Contains the logs of the previously dags run
    │   └── plugins      <- Should be empty : Contains all needed plugins to make the dag work

---

## Rapport

### Introduction

Ce rapport présente une analyse détaillée de l'atelier de création d'un datamart pour la récupération des données des taxis de New York City (NYC). L'objectif principal de cet atelier est de concevoir et de mettre en œuvre un système efficace pour collecter, stocker et gérer les données des taxis de NYC, afin de générer une connaissance pour une entreprise de VTC baséa à NYC.

### Objectifs

Le projet de création d'un datamart pour les données des taxis de New York City (NYC) vise à répondre à plusieurs objectifs essentiels, en alignement avec le plan du cours et les spécifications du projet :

1.  Automatisation de la récupération des données :

    -   Automatiser la récupération des données à partir du site de l'État de New York vers un Data Lake.
    -   Établir un processus ETL (Extract, Transform, Load) pour garantir la qualité et la propreté des données stockées dans un Data Warehouse.

2.  Création d'un Data Mart pour la visualisation de données :

    -   Appliquer un deuxième processus ETL pour préparer les données en vue de la visualisation dans un tableau de bord.
    -   Concevoir un modèle en flocon pour les données finales qui serviront de base au tableau de bord.
    -   Stocker les résultats de ces transformations dans une base de données OLAP dédiée au Data Mart.

3.  Visualisation et conception de tableau de bord :

    -   Réaliser une Analyse Exploratoire de Données (EDA) pour mieux comprendre les données stockées dans le Data Mart.
    -   Identifier et sélectionner les KPIs (Key Performance Indicators) pertinents pour la restitution des informations clés.
    -   Concevoir des visualisations de données à l'aide d'outils tels que Tableau ou Power BI.
    -   Fusionner les différentes visualisations de données pour créer un tableau de bord interactif et informatif.

4.  Introduction à l'automatisation des tâches avec Apache Airflow :

    -   Rédiger une DAG (Directed Acyclic Graph) dans Apache Airflow pour automatiser le processus de récupération du dernier mois de données.
    -   Planifier l'exécution de cette tâche pour qu'elle se déclenche automatiquement tous les 1ers du mois.

En résumé, les principaux objectifs de ce projet sont de mettre en place une infrastructure robuste pour la gestion des données des taxis de NYC, de garantir la qualité et la disponibilité des données pour l'analyse et la prise de décision, et d'initier les étudiants à des concepts avancés en matière de Business Intelligence et d'automatisation des tâches dans le domaine de l'informatique décisionnelle.

### Lancement du projet

### Méthodologie

#### Étape 1 : Récupération des données en local (TP1)

La première étape consistait à récupérer les données des taxis de NYC à partir du site de l'État de New York et à les stocker localement dans le répertoire `data/raw`. Pour cela, j'ai créé 2 fonctions dans le fichier `src/data/grab_parquet.py` :

-   `grab_data` - Récupère l'ensemble des datasets de janvier 2023 à décembre 2023. J'ai itéré à travers chaque mois et téléchargé les fichiers correspondants en utilisant la bibliothèque urllib pour accéder aux URL spécifiques des enregistrements de voyages des taxis de NYC.
-   `grab_last_data` - Récupère le dataset le plus récent. J'ai utilisé la bibliothèque pendulum pour gérer la date et remonter dans les mois afin de trouver le fichier le plus récent disponible.

Avant de télécharger chaque fichier, j'ai vérifié s'il existait déjà localement pour éviter les téléchargements redondants.

Le stockage des données dans le répertoire `data/raw` en local a permis de simplifier le processus initial de récupération des données et de garantir un contrôle total sur les fichiers téléchargés avant de les intégrer dans le Data Lake sur Minio.

### Étape 2 : Stockage des données locales en base de données (TP2)

La seconde étape consistait à stocker les données récupérées localement lors de l'étape précédente dans une base de données. Pour cela, j'ai d'abord utilisé Beekeeper pour créer une database appelée `nyc_warehouse`. Ensuite, j'ai lancé le programme `src/data/dump_to_sql.py` qui a récupéré tous les fichiers `.parquet` stockés dans le répertoire `data/raw` et a envoyé les données dans la base `nyc_warehouse`.

Pour améliorer les performances de cette opération, j'ai introduit la notion de chunking dans le programme. Cette approche divise le DataFrame en chunks de taille fixe avant de les écrire dans la base de données. Cela permet de réduire la charge sur la mémoire et d'améliorer l'efficacité de l'opération d'écriture. J'ai également ajouté des logs pour suivre l'avancée du processus.

### Architecture

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

---

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
