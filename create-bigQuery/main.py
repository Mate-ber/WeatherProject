from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

# Configuration
project_id = 'vibrant-map-454012-h9'
dataset_id = 'weather_data'
table_id = 'weather_records'
KEY_PATH = "../vibrant-map-454012-h9-051c9c818db3.json"  # Path to service account key file

# Load credentials from the service account key file
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)

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
table = client.create_table(table)  # This will create the table in BigQuery
print(f"Created table {table.full_table_id}")