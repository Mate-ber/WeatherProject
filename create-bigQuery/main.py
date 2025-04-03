from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import os

def create_bigquery_table():
    """
    Creates a BigQuery dataset and table for weather data.
    """
    # Configuration from environment variables
    project_id = os.getenv("PROJECT_ID")
    dataset_id = os.getenv("DATASET_ID")
    table_id = os.getenv("TABLE_ID")

    if not all([project_id, dataset_id, table_id]):
        raise ValueError("Missing required environment variables: PROJECT_ID, DATASET_ID, or TABLE_ID")

    # Load credentials from the mounted secret
    credentials = service_account.Credentials.from_service_account_file(
        "/etc/secrets/service-account-key.json",
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    # Initialize the BigQuery client with the credentials
    client = bigquery.Client(project=project_id, credentials=credentials)

    # Check if the dataset exists; create it if it doesn’t
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists.")
    except NotFound:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = 'US'  # Set to your preferred region, e.g., 'EU'
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")

    # Define the schema for the table
    schema = [
        bigquery.SchemaField('location', 'RECORD', fields=[
            bigquery.SchemaField('name', 'STRING'),
            bigquery.SchemaField('region', 'STRING'),
            bigquery.SchemaField('country', 'STRING'),
            bigquery.SchemaField('lat', 'FLOAT'),
            bigquery.SchemaField('lon', 'FLOAT'),
            bigquery.SchemaField('tz_id', 'STRING'),
            bigquery.SchemaField('localtime_epoch', 'INTEGER'),
            bigquery.SchemaField('localtime', 'TIMESTAMP'),
        ]),
        bigquery.SchemaField('current', 'RECORD', fields=[
            bigquery.SchemaField('last_updated_epoch', 'INTEGER'),
            bigquery.SchemaField('last_updated', 'TIMESTAMP'),
            bigquery.SchemaField('temp_c', 'FLOAT'),
            bigquery.SchemaField('temp_f', 'FLOAT'),
            bigquery.SchemaField('is_day', 'INTEGER'),
            bigquery.SchemaField('condition', 'RECORD', fields=[
                bigquery.SchemaField('text', 'STRING'),
                bigquery.SchemaField('icon', 'STRING'),
                bigquery.SchemaField('code', 'INTEGER'),
            ]),
            bigquery.SchemaField('wind_mph', 'FLOAT'),
            bigquery.SchemaField('wind_kph', 'FLOAT'),
            bigquery.SchemaField('wind_degree', 'INTEGER'),
            bigquery.SchemaField('wind_dir', 'STRING'),
            bigquery.SchemaField('pressure_mb', 'FLOAT'),
            bigquery.SchemaField('pressure_in', 'FLOAT'),
            bigquery.SchemaField('precip_mm', 'FLOAT'),
            bigquery.SchemaField('precip_in', 'FLOAT'),
            bigquery.SchemaField('humidity', 'INTEGER'),
            bigquery.SchemaField('cloud', 'INTEGER'),
            bigquery.SchemaField('feelslike_c', 'FLOAT'),
            bigquery.SchemaField('feelslike_f', 'FLOAT'),
            bigquery.SchemaField('windchill_c', 'FLOAT'),
            bigquery.SchemaField('windchill_f', 'FLOAT'),
            bigquery.SchemaField('heatindex_c', 'FLOAT'),
            bigquery.SchemaField('heatindex_f', 'FLOAT'),
            bigquery.SchemaField('dewpoint_c', 'FLOAT'),
            bigquery.SchemaField('dewpoint_f', 'FLOAT'),
            bigquery.SchemaField('vis_km', 'FLOAT'),
            bigquery.SchemaField('vis_miles', 'FLOAT'),
            bigquery.SchemaField('uv', 'FLOAT'),
            bigquery.SchemaField('gust_mph', 'FLOAT'),
            bigquery.SchemaField('gust_kph', 'FLOAT'),
        ]),
    ]

    # Create the table
    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    try:
        table = client.create_table(table)
        print(f"Created table {table.full_table_id}")
    except Exception as e:
        print(f"Table creation failed: {e}")
        if "Already Exists" in str(e):
            print(f"Table {table.full_table_id} already exists.")
        else:
            raise e

if __name__ == "__main__":
    create_bigquery_table()