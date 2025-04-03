from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account

# Project and table configuration
PROJECT_ID = "vibrant-map-454012-h9"
DATASET_ID = "weather_data"
TABLE_ID = "weather_records"
KEY_PATH = "../vibrant-map-454012-h9-051c9c818db3.json"
BUCKET_NAME = "weather_data_lake"

def read_cities():
    """
    Reads the list of cities from cities.txt.
    Returns a list of city names.
    """
    with open("cities.txt", "r") as file:
        cities = [line.strip() for line in file if line.strip()]
    return cities

def get_newest_file(city, bucket):
    """
    Finds the newest JSON file for a given city in the GCS bucket.
    Returns the URI of the newest file (e.g., gs://weather_data_lake/weather_data/London/2025-04-03T10:03:34.946789.json).
    """
    # List all files in the city's folder
    prefix = f"weather_data/{city}/"
    blobs = bucket.list_blobs(prefix=prefix)

    # Filter for JSON files and sort by name (timestamp)
    json_files = [blob for blob in blobs if blob.name.endswith(".json")]
    if not json_files:
        raise FileNotFoundError(f"No JSON files found for {city} in {prefix}")

    # Sort by name (newest timestamp will be last)
    json_files.sort(key=lambda x: x.name)
    newest_file = json_files[-1]  # Get the newest file
    newest_file_uri = f"gs://{BUCKET_NAME}/{newest_file.name}"
    return newest_file_uri

def load_to_bigquery():
    """
    Loads the newest weather data file from GCS to BigQuery for each city, then deduplicates.
    """
    # Read cities from the file
    CITIES = read_cities()
    print(f"Cities to load: {CITIES}")

    # Authenticate with the service account
    try:
        credentials = service_account.Credentials.from_service_account_file(
            KEY_PATH,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

        # Initialize the BigQuery client with the credentials
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        print(f"Successfully connected to BigQuery with project: {PROJECT_ID}")

        # Initialize the GCS client with the same credentials
        storage_client = storage.Client(project=PROJECT_ID, credentials=credentials)
        bucket = storage_client.get_bucket(BUCKET_NAME)
        print(f"Successfully connected to GCS bucket: {BUCKET_NAME}")
    except Exception as e:
        print(f"Failed to connect to BigQuery or GCS: {e}")
        return

    # Configure the BigQuery table reference
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,  # Format of the source files
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append the newest data
        autodetect=True  # Automatically detect the schema
    )

    # Loop through each city and load its newest file
    for city in CITIES:
        try:
            # Get the URI of the newest file for the city
            newest_file_uri = get_newest_file(city, bucket)
            print(f"Loading newest data for {city} from {newest_file_uri}...")

            # Load the newest file into BigQuery
            load_job = client.load_table_from_uri(
                newest_file_uri,  # Source URI for the newest file
                table_ref,  # Destination table reference
                job_config=job_config  # Load job configuration
            )
            load_job.result()  # Wait for the job to complete
            print(f"Successfully loaded newest data for {city}.")
        except Exception as e:
            print(f"Failed to load newest data for {city}: {e}")

if __name__ == "__main__":
    load_to_bigquery()