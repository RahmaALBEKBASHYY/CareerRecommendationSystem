import os
import pandas as pd
import requests
import logging
from pandas import json_normalize
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import datetime
import azure.functions as func  # Import necessary for Azure Functions

logging.basicConfig(level=logging.INFO)

app = func.FunctionApp()

@app.schedule(schedule="0 3 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def timer_trigger_api(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

    client_id = os.getenv('CLIENT_ID', 'PAR_careerrecommendation_aa0c42f82afe1bbfcf7c92fc1408a40bfd42be8d70eaae54bb76912386925981')
    client_secret = os.getenv('CLIENT_SECRET', '13b771dc0d410c50cf319c113c82c5693c46d402645728b3bd333e8ec8b0903a')
    token_url = os.getenv('TOKEN_URL', 'https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire')
    scope = os.getenv('SCOPE', 'api_offresdemploiv2 o2dsoffre')

    global_df = pd.DataFrame()

    def add_to_dataframe(data):
        nonlocal global_df
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, list):
                    if value is not None:  # Check if value is None before normalizing
                        try:
                            df = json_normalize(value)
                            global_df = pd.concat([global_df, df], ignore_index=True)
                        except Exception as e:
                            logging.error(f"Impossible de normaliser {key}: {e}")
                elif isinstance(value, dict):
                    add_to_dataframe(value)
    try:
        payload = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': scope
        }
        response = requests.post(token_url, data=payload)
        response.raise_for_status()
        token_info = response.json()

        access_token = token_info.get('access_token')
        if not access_token:
            raise ValueError("Token d'accès non trouvé.")
        logging.info('Access token obtained successfully.')
    except requests.RequestException as e:
        logging.error(f"Erreur d'authentification : {e}")
        return

    if access_token:
        search_url = 'https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search'
        params = {'q': 'serveur', 'location': 'Paris'}
        headers = {'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'}
        
        while True:
            try:
                search_response = requests.get(search_url, headers=headers, params=params)
                search_response.raise_for_status()
                data = search_response.json()
                add_to_dataframe(data)
                
                search_url = data.get('next')
                if not search_url:
                    logging.info("Fin des données ou pagination non spécifiée.")
                    break
            except requests.RequestException as e:
                logging.error(f"Erreur lors de la requête: {e}")
                break

    global_df.fillna('Non spécifié', inplace=True)

    def normalize_column_types(df):
        for column in df.columns:
            if df[column].apply(lambda x: isinstance(x, str) and x.strip() == 'Non spécifié').any():
                df[column] = df[column].replace('Non spécifié', pd.NA)
            try:
                df[column] = pd.to_numeric(df[column], errors='ignore')
            except Exception as e:
                logging.error(f"Erreur de conversion pour la colonne {column}: {e}")
            if df[column].apply(lambda x: isinstance(x, str) and x.lower() in ['true', 'false']).any():
                df[column] = df[column].apply(lambda x: x.lower() == 'true' if isinstance(x, str) else x)
        return df

    global_df = normalize_column_types(global_df)

    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    container_name = 'staging'
    blob_name = 'FranceTravailAPI.csv'  # Change to CSV
    
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        
        buffer = BytesIO()
        global_df.to_csv(buffer, index=False)  # Save as CSV instead of Parquet
        buffer.seek(0)
        
        blob_client.upload_blob(buffer, overwrite=True)
        logging.info(f"Les données ont été sauvegardées dans {blob_name}.")
    except Exception as e:
        logging.error(f"Erreur lors de l'enregistrement du fichier CSV : {e}")

    logging.info('Python timer trigger function executed.')
