# ============================================
# IMPORTS — les librairies dont on a besoin
# ============================================
from airflow import DAG                          # la classe principale d'Airflow
from airflow.operators.python import PythonOperator  # pour exécuter des fonctions Python
from datetime import datetime, timedelta         # pour gérer les dates
import requests                                  # pour appeler l'API
import pandas as pd                              # pour manipuler les données
import psycopg2                                  # pour se connecter à PostgreSQL


# ============================================
# CONFIGURATION DE LA CONNEXION POSTGRESQL
# ============================================
DB_CONFIG = {
    "host": "postgres",        # nom du conteneur PostgreSQL dans Docker
    "database": "pipeline_db", # nom de notre base de données
    "user": "licia",           # notre utilisateur
    "password": "password123"  # notre mot de passe
}


# ============================================
# TÂCHE 1 : Extraire les données de l'API météo
# ============================================
def extraire_meteo():
    """
    Cette fonction appelle l'API OpenMeteo
    et récupère la météo de Paris
    """
    print("Début extraction météo...")

    # L'URL de l'API avec les paramètres qu'on veut
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 48.8566,      # latitude de Paris
        "longitude": 2.3522,      # longitude de Paris
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "forecast_days": 1        # météo du jour uniquement
    }

    # On appelle l'API
    response = requests.get(url, params=params)

    # On vérifie que ça a marché
    if response.status_code == 200:
        print("API appelée avec succès !")
        data = response.json()
        print(f"Données reçues : {len(data['hourly']['time'])} heures")
        return data
    else:
        raise Exception(f"Erreur API : {response.status_code}")


# ============================================
# TÂCHE 2 : Nettoyer et transformer les données
# ============================================
def transformer_meteo():
    """
    Cette fonction nettoie les données
    et les met dans un format propre
    """
    print("Début transformation...")

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 48.8566,
        "longitude": 2.3522,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "forecast_days": 1
    }
    response = requests.get(url, params=params)
    data = response.json()

    df = pd.DataFrame({
        "datetime": data["hourly"]["time"],
        "temperature": data["hourly"]["temperature_2m"],
        "precipitation": data["hourly"]["precipitation"],
        "vitesse_vent": data["hourly"]["windspeed_10m"],
        "ville": "Paris",
        "created_at": datetime.now()
    })

    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.dropna()

    print(f"Données transformées : {len(df)} lignes")
    print(df.head())

    # On ne retourne plus rien — on supprime le return

# ============================================
# TÂCHE 3 : Charger les données dans PostgreSQL
# ============================================
def charger_meteo():
    """
    Cette fonction crée la table si elle n'existe pas
    et insère les données dans PostgreSQL
    """
    print("Début chargement dans PostgreSQL...")

    # On se connecte à PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # On crée la table si elle n'existe pas encore
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS meteo_paris (
            id SERIAL PRIMARY KEY,
            datetime TIMESTAMP,
            temperature FLOAT,
            precipitation FLOAT,
            vitesse_vent FLOAT,
            ville VARCHAR(50),
            created_at TIMESTAMP
        )
    """)

    # On récupère les données transformées
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 48.8566,
        "longitude": 2.3522,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "forecast_days": 1
    }
    response = requests.get(url, params=params)
    data = response.json()

    df = pd.DataFrame({
        "datetime": data["hourly"]["time"],
        "temperature": data["hourly"]["temperature_2m"],
        "precipitation": data["hourly"]["precipitation"],
        "vitesse_vent": data["hourly"]["windspeed_10m"],
        "ville": "Paris",
        "created_at": datetime.now()
    })
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.dropna()

    # On insère chaque ligne dans la table
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO meteo_paris
            (datetime, temperature, precipitation, vitesse_vent, ville, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row["datetime"],
            row["temperature"],
            row["precipitation"],
            row["vitesse_vent"],
            row["ville"],
            row["created_at"]
        ))

    # On valide les insertions
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Données chargées avec succès : {len(df)} lignes insérées !")


# ============================================
# DÉFINITION DU DAG
# ============================================
# C'est ici qu'on configure le pipeline lui-même

# Arguments par défaut pour toutes les tâches
default_args = {
    "owner": "licia",              # le propriétaire du DAG
    "retries": 1,                  # si une tâche échoue, réessaie 1 fois
    "retry_delay": timedelta(minutes=5),  # attend 5 minutes avant de réessayer
    "email_on_failure": False,     # pas d'email en cas d'échec
}

# On crée le DAG
with DAG(
    dag_id="weather_pipeline",           # l'identifiant unique du DAG
    description="Pipeline météo Paris",  # description
    default_args=default_args,
    start_date=datetime(2024, 1, 1),     # date de début
    schedule_interval="@hourly",         # s'exécute toutes les heures
    catchup=False,                       # ne rattrape pas les exécutions passées
    tags=["meteo", "pipeline", "paris"]  # tags pour filtrer dans l'interface
) as dag:

    # On définit les 3 tâches
    tache_extraction = PythonOperator(
        task_id="extraire_meteo",        # identifiant de la tâche
        python_callable=extraire_meteo,  # la fonction à exécuter
    )

    tache_transformation = PythonOperator(
        task_id="transformer_meteo",
        python_callable=transformer_meteo,
    )

    tache_chargement = PythonOperator(
        task_id="charger_meteo",
        python_callable=charger_meteo,
    )

    # On définit l'ordre d'exécution
    # >> veut dire "puis"
    tache_extraction >> tache_transformation >> tache_chargement