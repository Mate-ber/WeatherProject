from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import os
import logging
import uuid
from datetime import datetime

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
            for blob in blobs:
                if blob.name.endswith(".json") and not blob.name.endswith(".processed"):
                    try:
                        # Download and parse the JSON data
                        data = blob.download_as_text()
                        parsed_data = eval(data)  # Assuming JSON is a valid Python dict
                        location_name = parsed_data.get("location", {}).get("name")
                        localtime_epoch = parsed_data.get("location", {}).get("localtime_epoch")

                        if location_name and localtime_epoch:
                            # Check for existing records in BigQuery
                            query = f"""
                            SELECT COUNT(*)
                            FROM `{full_table_id}`
                            WHERE location.name = '{location_name}'
                            AND location.localtime_epoch = {localtime_epoch}
                            """
                            query_job = client.query(query)
                            result = next(query_job.result())[0]
                            if result > 0:
                                logger.info(f"Data for {location_name} at {localtime_epoch} already exists. Skipping.")
                                bucket.rename_blob(blob, f"{blob.name}.processed")
                                continue

                        # Add a unique record ID
                        parsed_data["record_id"] = str(uuid.uuid4())

                        # Format timestamps if necessary
                        if "location" in parsed_data and "localtime" in parsed_data["location"]:
                            try:
                                parsed_data["location"]["localtime"] = datetime.strptime(
                                    parsed_data["location"]["localtime"], "%Y-%m-%d %H:%M"
                                ).isoformat()
                            except ValueError:
                                logger.warning(f"Invalid localtime format for {location_name}")
                        if "current" in parsed_data and "last_updated" in parsed_data["current"]:
                            try:
                                parsed_data["current"]["last_updated"] = datetime.strptime(
                                    parsed_data["current"]["last_updated"], "%Y-%m-%d %H:%M"
                                ).isoformat()
                            except ValueError:
                                logger.warning(f"Invalid last_updated format for {location_name}")

                        # Insert the new record into BigQuery
                        table = client.get_table(full_table_id)
                        errors = client.insert_rows_json(table, [parsed_data])
                        if errors:
                            logger.error(f"Failed to load {blob.name}: {errors}")
                        else:
                            logger.info(f"Successfully loaded {blob.name} into BigQuery")
                            bucket.rename_blob(blob, f"{blob.name}.processed")
                    except Exception as e:
                        logger.error(f"Error processing {blob.name}: {e}")
                        continue
        except Exception as e:
            logger.error(f"Failed to process data for {city}: {e}")

if __name__ == "__main__":
    load_to_bigquery()
