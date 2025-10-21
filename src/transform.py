import logging

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_taxi_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les données des taxis :
    - Supprime les NaN
    - Supprime les valeurs négatives
    - Valide les données
    """
    initial_count = len(df)
    logger.info(f"Nettoyage des données - Lignes initiales: {initial_count}")

    # Suppression des lignes avec des NaN dans les colonnes critiques
    critical_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
    ]
    df = df.dropna(subset=[col for col in critical_columns if col in df.columns])
    logger.info(
        f"Après suppression NaN: {len(df)} lignes ({initial_count - len(df)} supprimées)"
    )

    # Suppression des valeurs négatives pour les colonnes numériques
    numeric_columns = [
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "total_amount",
    ]

    for col in numeric_columns:
        if col in df.columns:
            before = len(df)
            df = df[df[col] >= 0]
            removed = before - len(df)
            if removed > 0:
                logger.info(
                    f"Valeurs négatives supprimées pour {col}: {removed} lignes"
                )

    # Suppression des trajets avec une distance nulle mais un montant positif (fraude potentielle)
    if "trip_distance" in df.columns and "fare_amount" in df.columns:
        before = len(df)
        df = df[~((df["trip_distance"] == 0) & (df["fare_amount"] > 0))]
        removed = before - len(df)
        if removed > 0:
            logger.info(
                f"Trajets suspects supprimés (distance=0, montant>0): {removed} lignes"
            )

    # Suppression des trajets avec un nombre de passagers invalide
    if "passenger_count" in df.columns:
        before = len(df)
        df = df[(df["passenger_count"] > 0) & (df["passenger_count"] <= 8)]
        removed = before - len(df)
        if removed > 0:
            logger.info(f"Nombre de passagers invalide supprimé: {removed} lignes")

    # Validation des dates (pickup < dropoff)
    if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
        before = len(df)
        df = df[df["tpep_pickup_datetime"] < df["tpep_dropoff_datetime"]]
        removed = before - len(df)
        if removed > 0:
            logger.info(f"Dates invalides supprimées: {removed} lignes")

    final_count = len(df)
    logger.info(
        f"Nettoyage terminé - Lignes finales: {final_count} ({initial_count - final_count} supprimées au total)"
    )

    return df
