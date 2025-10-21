import logging
from typing import Any, Dict, Iterator

import dlt
import pandas as pd
import requests

from transform import clean_taxi_data

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def download_taxi_data(year: int = 2025) -> Iterator[Dict[str, Any]]:
    """
    Télécharge les fichiers parquet des taxis jaunes de NYC pour une année donnée.
    Gère les erreurs pour les fichiers manquants.
    """
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    for month in range(1, 13):
        # Format: yellow_tripdata_2025-01.parquet
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        url = f"{base_url}/{filename}"

        try:
            logger.info(f"Téléchargement de {filename}...")
            response = requests.get(url, timeout=60)
            response.raise_for_status()

            # Sauvegarder temporairement le fichier
            temp_file = f"/tmp/{filename}"
            with open(temp_file, "wb") as f:
                f.write(response.content)

            # Lire le fichier parquet
            df = pd.read_parquet(temp_file)
            logger.info(f"✓ {filename} téléchargé - {len(df)} lignes")

            # Nettoyage des données
            df_clean = clean_taxi_data(df)
            logger.info(
                f"✓ {filename} nettoyé - {len(df_clean)} lignes après nettoyage"
            )

            # Convertir en dictionnaires pour dlt
            yield df_clean.to_dict(orient="records")

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(
                    f"✗ {filename} non trouvé (404) - fichier probablement pas encore disponible"
                )
            else:
                logger.error(f"✗ Erreur HTTP pour {filename}: {e}")
        except requests.exceptions.Timeout:
            logger.error(f"✗ Timeout lors du téléchargement de {filename}")
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Erreur de requête pour {filename}: {e}")
        except Exception as e:
            logger.error(f"✗ Erreur inattendue pour {filename}: {e}")


@dlt.resource(name="yellow_taxi_trips", write_disposition="append")
def taxi_trips_resource():
    """Resource dlt pour les trajets de taxis"""
    yield from download_taxi_data(year=2025)


def main():
    """
    Fonction principale pour exécuter le pipeline dlt
    """
    pipeline = dlt.pipeline(
        pipeline_name="nyc_taxi_pipeline",
        destination="duckdb",
        dataset_name="nyc_taxi_data",
    )

    logger.info("Démarrage du pipeline dlt...")

    load_info = pipeline.run(taxi_trips_resource(), table_name="yellow_taxi_trips")

    logger.info("Pipeline terminé!")
    logger.info(f"Informations de chargement: {load_info}")


if __name__ == "__main__":
    main()
