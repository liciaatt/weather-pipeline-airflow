# Weather Pipeline — Data Engineering Project

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.9.1-red)
![Docker](https://img.shields.io/badge/Docker-29.1-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)

## Description
Pipeline de données automatisé qui collecte les données météo 
de Paris toutes les heures via l'API OpenMeteo et les stocke 
dans PostgreSQL, orchestré par Apache Airflow et containerisé 
avec Docker.

## Architecture

## Stack technique
- **Orchestration** : Apache Airflow 2.9.1
- **Containerisation** : Docker & Docker Compose
- **Base de données** : PostgreSQL 15
- **Langage** : Python 3.12
- **Librairies** : Pandas, Requests, Psycopg2

## Pipeline
Le DAG `weather_pipeline` s'exécute toutes les heures et effectue :

1. **Extraction** — Appel API OpenMeteo (météo Paris)
2. **Transformation** — Nettoyage avec Pandas
3. **Chargement** — Insertion dans PostgreSQL

## Données collectées
| Champ | Description |
|-------|-------------|
| datetime | Heure de la mesure |
| temperature | Température en °C |
| precipitation | Précipitations en mm |
| vitesse_vent | Vitesse du vent en km/h |
| ville | Ville (Paris) |

## Comment lancer le projet

### Prérequis
- Docker
- Docker Compose

### Installation
```bash
# Clone le repository
git clone https://github.com/liciaatt/weather-pipeline-airflow.git
cd weather-pipeline-airflow

# Donne les permissions nécessaires
chmod -R 777 logs dags scripts

# Lance tous les services
docker-compose up -d
```
API OpenMeteo → Python → Airflow → PostgreSQL

### Accès Airflow
- URL : http://localhost:8080
- Username : admin
- Password : admin

### Vérifier les données dans PostgreSQL
```bash
docker exec -it postgres_db psql -U licia -d pipeline_db
SELECT COUNT(*) FROM meteo_paris;
SELECT * FROM meteo_paris LIMIT 5;
```

## Auteur
**Licia Attouche** — Data Analyst & Data Engineer
- LinkedIn : [ton linkedin]
- GitHub : [liciaatt](https://github.com/liciaatt)