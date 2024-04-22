# ATL-Datamart

Atelier de Big Data : Introduction aux pipelines de données et Architecture de donnes pour DC Paris

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

Pour cette étape, j'ai utilisé les bibliothèques Python suivantes :
- `urllib` : Pour récupérer les données en utilisant les URL spécifiques des enregistrements de voyages des taxis de NYC.
- `pendulum` : Pour gérer la date et remonter dans les mois afin de trouver le fichier le plus récent disponible.
- `os` : Pour effectuer des opérations sur les chemins de fichiers et les répertoires, comme la vérification de l'existence de fichiers locaux avant le téléchargement.


### Étape 2 : Stockage des données locales en base de données (TP2)

La seconde étape consistait à stocker les données récupérées localement lors de l'étape précédente dans une base de données. Pour cela, j'ai d'abord utilisé Beekeeper pour créer une database appelée `nyc_warehouse`. Ensuite, j'ai lancé le programme `src/data/dump_to_sql.py` qui a récupéré tous les fichiers `.parquet` stockés dans le répertoire `data/raw` et a envoyé les données dans la base `nyc_warehouse`.

Pour améliorer les performances de cette opération, j'ai introduit la notion de chunking dans le programme. Cette approche divise le DataFrame en chunks de taille fixe avant de les écrire dans la base de données. Cela permet de réduire la charge sur la mémoire et d'améliorer l'efficacité de l'opération d'écriture. J'ai également ajouté des logs pour suivre l'avancée du processus.

De plus, j'ai externalisé la connexion à la base de données dans le fichier `database_operations.py` pour une meilleure lisibilité du code. 

Pour cela, j'ai utilisé les outils et bibliothèques suivants :

- `Beekeeper` : Pour créer une base de données appelée nyc_warehouse.
- `SQLAlchemy` : Pour interagir avec la base de données et exécuter des requêtes SQL.
- `pandas` : Pour lire les fichiers Parquet locaux et les charger dans des DataFrames.
- `os` : Pour obtenir la liste des fichiers Parquet stockés localement et itérer sur eux.
- `logging` : Pour journaliser les étapes du processus de stockage des données.


### Étape 3 : Utilisation de Minio (TP1 & TP2)

Étant donné que la gestion des données en local fonctionne, je suis passée à l'utilisation de Minio. Pour cela, j'ai d'abord complété la fonction `write_data_minio` dans le fichier `src/data/grab_parquet.py`. Cette fonction a pour but de récupérer tous les fichiers `.parquet` du répertoire `data/raw` et de les sauvegarder dans Minio. Si un fichier existe déjà dans Minio, je ne le sauvegarde pas à nouveau pour des questions de performance.

Ensuite, je suis passée à la récupération des données de Minio afin de les utiliser pour remplir la database. Pour cela, j'ai modifié le fichier `dump_to_sql.py`. Dans celui-ci, je tente d'abord de récupérer les fichiers depuis Minio (`get_parquet_files_from_minio`), si je ne les ai pas, j'utilise les fichiers en local (`get_parquet_files_locally`).

Pour la connexion à minio, j'ai créé un programme dans le fichier `minio_operations`. Cela permet de factoriser le code pour une meilleure lisibilité mais aussi de changer le code à un seul et unique endroit si nécessaire.

Pour cela, j'ai utilisé les bibliothèques suivantes :
- `Minio` : Pour interagir avec Minio et stocker les fichiers Parquet.
- `urllib` : Pour récupérer les données localement si elles ne sont pas disponibles dans Minio.
- `os` : Pour obtenir la liste des fichiers Parquet locaux et les charger dans Minio.
- `logging` : Pour journaliser les étapes du processus de sauvegarde dans Minio.

### Étape 4 : Création des tables en modèle en Flocon (TP3)
Dans cette étape, j'ai utilisé les requêtes SQL pour créer les tables en modèle en Flocon. Voici les principales étapes que j'ai suivies :

- Analyse des données: J'ai examiné les données disponibles et identifié les dimensions et les faits pertinents pour concevoir le modèle en Flocon dans un SGBD `PostgreSQL`.
- Conception du modèle en Flocon: En utilisant les connaissances acquises lors de l'analyse des données, j'ai conçu un modèle en Flocon qui comprend une table de faits et des tables de dimensions.
- Écriture des requêtes SQL: J'ai écrit des requêtes SQL pour créer les tables en Flocon dans le fichier `src/data/creation.sql`.
- Test et validation: Une fois les requêtes SQL écrites, j'ai exécuté ces scripts pour créer les tables en Flocon dans le SGBD. J'ai ensuite vérifié que les tables étaient correctement créées en examinant leur structure.

### Étape 5 : Insertion des données dans le Data Mart (TP3)
Dans cette étape, j'ai inséré les données provenant de la base de données Data Warehouse dans la base de données Data Mart. Voici les étapes que j'ai suivies :

- Analyse des données: J'ai examiné les données disponibles dans la base de données Data Warehouse et identifié les données à insérer dans le Data Mart.
- Écriture des requêtes SQL: J'ai écrit des requêtes SQL pour extraire les données de la base de données Data Warehouse et les insérer dans les tables correspondantes du Data Mart dans le fichier `insertion.sql`. Ces requêtes incluaient des instructions INSERT INTO SELECT pour copier les données d'une base de données à l'autre.
- Exécution des requêtes: J'ai exécuté les requêtes SQL pour insérer les données dans le Data Mart. J'ai surveillé l'avancement de l'opération et vérifié que toutes les données étaient correctement transférées.
Test et validation: Une fois les données insérées, j'ai comparé les données du Data Mart avec celles de la base de données Data Warehouse pour m'assurer qu'elles correspondaient.


### Étape 6 : Exploration Data Analysis (TP4)
Dans cette étape, j'ai effectué une Analyse Exploratoire des Données (EDA) pour mieux comprendre les données stockées dans le Data Mart. Voici les étapes que j'ai suivies :

- Analyse des données: J'ai examiné les données disponibles dans le Data Mart et identifié les variables d'intérêt ainsi que les relations potentielles entre ces variables.
- Visualisation des données: J'ai créé des visualisations graphiques telles que des histogrammes, des diagrammes de dispersion et des graphiques linéaires pour explorer les distributions et les corrélations des données.
- Identification des KPIs: J'ai identifié les Key Performance Indicators (KPIs) pertinents pour évaluer les performances du système et guider la prise de décision.


### Étape 7 : Conception du tableau de bord (TP4)
Dans cette étape, j'ai conçu un tableau de bord pour visualiser les résultats de l'analyse des données. Voici les principales étapes que j'ai suivies :

- Sélection des outils de visualisation: J'ai choisi d'utiliser `matplotlib` pour créer le tableau de bord.
- Création des visualisations: J'ai créé le tableau de bord à partir des visualisations graphiques.  
- Intégration des KPIs: J'ai intégré les KPIs identifiés lors de l'EDA dans le tableau de bord pour fournir une vue d'ensemble.


### Étape 8 : Automatisation des tâches avec Apache Airflow (TP5)
Dans cette étape, j'ai utilisé Apache Airflow pour automatiser le processus de récupération du dernier mois de données. Voici les étapes que j'ai suivies :

- Création d'une DAG: J'ai créé une DAG dans Apache Airflow pour définir le flux de travail automatisé. La DAG comprend des tâches pour télécharger le dernier mois de données et les stocker dans Minio.
- Planification des tâches: J'ai planifié l'exécution de la DAG pour qu'elle se déclenche automatiquement tous les premiers du mois, en utilisant des expressions cron pour définir l'horaire de déclenchement.
- Configuration des connexions: J'ai configuré les connexions Airflow pour se connecter au système de stockage Minio et accéder aux données.
Test et validation: J'ai testé la DAG pour m'assurer qu'elle fonctionnait correctement et qu'elle récupérait les données attendues chaque mois.


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
