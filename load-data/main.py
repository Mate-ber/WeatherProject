from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import os
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_cities():
    """Reads the list of cities from cities.txt."""
    with open("cities.txt", "r") as file:
        cities = [line.strip() for line in file if line.strip()]
    return cities

def load_to_bigquery():
    """Loads weather data from GCS to BigQuery, avoiding duplicates."""
    # Load configuration from environment variables
    project_id = os.getenv("PROJECT_ID")
    dataset_id = os.getenv("DATASET_ID")
    table_id = os.getenv("TABLE_ID")
    bucket_name = os.getenv("BUCKET_NAME")

    if not all([project_id, dataset_id, table_id, bucket_name]):
        raise ValueError("Missing required environment variables: PROJECT_ID, DATASET_ID, TABLE_ID, or BUCKET_NAME")

    # Read cities from the file
    cities = read_cities()
    logger.info(f"Cities to load: {cities}")

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
        logger.info(f"Successfully connected to GCS bucket: {bucket_name}")
    except Exception as e:
        logger.error(f"Failed to connect to BigQuery or GCS: {e}")
        raise e

    # Full table reference
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    for city in cities:
        try:
            # List all unprocessed files for the city
            prefix = f"weather_data/{city}/"
            blobs = bucket.list_blobs(prefix=prefix)
            found_files = False
            for blob in blobs:
                if blob.name.endswith(".json") and not blob.name.endswith(".processed"):
                    found_files = True
                    try:
                        # Download and parse the JSON data
                        data = blob.download_as_text()
                        parsed_data = json.loads(data)  # Use json.loads for safer parsing
                        location_name = parsed_data.get("location", {}).get("name")
                        localtime_epoch = parsed_data.get("location", {}).get("localtime_epoch")

                        if not (location_name and localtime_epoch):
                            logger.warning(f"Missing location.name or localtime_epoch in {blob.name}. Skipping.")
                            continue

                        # Check for existing records in BigQuery
                        query = f"""
                        SELECT COUNT(*)
                        FROM `{full_table_id}`
                        WHERE location.name = @location_name
                        AND location.localtime_epoch = @localtime_epoch
                        """
                        job_config = bigquery.QueryJobConfig(
                            query_parameters=[
                                bigquery.ScalarQueryParameter("location_name", "STRING", location_name),
                                bigquery.ScalarQueryParameter("localtime_epoch", "INT64", localtime_epoch),
                            ]
                        )
                        query_job = client.query(query, job_config=job_config)
                        result = next(query_job.result())[0]

                        if result > 0:
                            logger.info(f"Data for {location_name} at {localtime_epoch} already exists in BigQuery. Skipping.")
                            bucket.rename_blob(blob, f"{blob.name}.processed")
                            continue

                        # If not a duplicate, load the data into BigQuery
                        table_ref = client.dataset(dataset_id).table(table_id)
                        job_config = bigquery.LoadJobConfig(
                            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                            autodetect=True
                        )

                        # Write the data to a temporary file in memory to use load_table_from_uri
                        with open("temp.json", "w") as temp_file:
                            temp_file.write(json.dumps(parsed_data))

                        # Upload the temp file to a temporary location in GCS
                        temp_blob = bucket.blob(f"temp/{blob.name}")
                        temp_blob.upload_from_filename("temp.json")

                        # Load the single file into BigQuery
                        uri = f"gs://{bucket_name}/temp/{blob.name}"
                        load_job = client.load_table_from_uri(
                            uri,
                            table_ref,
                            job_config=job_config
                        )
                        load_job.result()

                        # Clean up: delete the temp file from GCS
                        temp_blob.delete()

                        logger.info(f"Successfully loaded {blob.name} into BigQuery")
                        bucket.rename_blob(blob, f"{blob.name}.processed")

                    except Exception as e:
                        logger.error(f"Error processing {blob.name}: {e}")
                        continue

            if not found_files:
                logger.info(f"No unprocessed files found for {city} in Cloud Storage.")

        except Exception as e:
            logger.error(f"Failed to process data for {city}: {e}")

if __name__ == "__main__":
    load_to_bigquery()
