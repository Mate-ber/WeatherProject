from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import os

def read_cities():
    with open("cities.txt", "r") as file:
        cities = [line.strip() for line in file if line.strip()]
    return cities

def load_to_bigquery(event, context):
    """Cloud Function entry point to load weather data from GCS to BigQuery."""
    # Load configuration from environment variables
    project_id = os.getenv("PROJECT_ID")
    dataset_id = os.getenv("DATASET_ID")
    table_id = os.getenv("TABLE_ID")
    bucket_name = os.getenv("BUCKET_NAME")

    if not all([project_id, dataset_id, table_id, bucket_name]):
        raise ValueError("Missing required environment variables: PROJECT_ID, DATASET_ID, TABLE_ID, or BUCKET_NAME")

    # Read cities from the file
    cities = read_cities()
    print(f"Cities to load: {cities}")

    # Authenticate with the service account
    try:
        credentials = service_account.Credentials.from_service_account_file(
            "/etc/secrets/service-account-key.json",
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

        # Initialize BigQuery and GCS clients
        client = bigquery.Client(project=project_id, credentials=credentials)
        storage_client = storage.Client(project=project_id, credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        print(f"Successfully connected to GCS bucket: {bucket_name}")
    except Exception as e:
        print(f"Failed to connect to BigQuery or GCS: {e}")
        return "Failed to connect to BigQuery or GCS", 500

    # Configure the BigQuery table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )

    # Load all files for each city
    for city in cities:
        try:
            uri = f"gs://{bucket_name}/weather_data/{city}/*.json"
            print(f"Loading all data for {city} from {uri}...")
            load_job = client.load_table_from_uri(
                uri,
                table_ref,
                job_config=job_config
            )
            load_job.result()
            print(f"Successfully loaded all data for {city}.")
        except Exception as e:
            print(f"Failed to load data for {city}: {e}")

    return "Weather data loaded successfully", 200