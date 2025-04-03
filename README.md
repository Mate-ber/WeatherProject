# Weather Data Pipeline on Google Cloud

## Overview
This project automates the collection and storage of weather data using Google Cloud services. The pipeline fetches weather data from an external API, stores it in Cloud Storage, and loads it into BigQuery for analysis. The process is orchestrated using Cloud Scheduler, Cloud Functions, and Cloud Run jobs.

## Approach
The approach leverages a serverless architecture to automate weather data collection and storage. Data is fetched from an external API by a Cloud Run job (`get-data-job`), temporarily stored in Cloud Storage, and then loaded into BigQuery by another Cloud Run job (`load-data-job`). A Cloud Function (`trigger-cloud-run-job`) acts as a bridge, triggered by Cloud Scheduler to initiate the jobs hourly, ensuring minimal maintenance and scalability.

## Table of Contents
1. [Architecture](#architecture)
2. [Prerequisites](#prerequisites)
3. [Setup Instructions](#setup-instructions)
4. [Deployment](#deployment)
5. [Testing](#testing)
6. [Troubleshooting](#troubleshooting)
7. [Analytics](#analytics)

## Architecture
- **Cloud Scheduler**: Triggers the pipeline hourly.
- **Cloud Functions**: Receives Pub/Sub messages from Cloud Scheduler and triggers Cloud Run jobs.
- **Cloud Run Jobs**:
  - `get-data-job`: Fetches weather data from an API (e.g., OpenWeatherMap) and stores it in Cloud Storage.
  - `load-data-job`: Loads data from Cloud Storage into BigQuery.
- **Cloud Storage**: Stores the fetched weather data temporarily.
- **BigQuery**: Stores the weather data for querying and analysis.

## Prerequisites
Before starting, ensure you have:
- A Google Cloud account with billing enabled.
- The `gcloud` CLI installed and authenticated (`gcloud auth login`).
- Docker installed for building and pushing container images.
- A weather API key (e.g., from OpenWeatherMap).

## Setup Instructions
### 1. Project Setup

Create a Google Cloud Project:
```bash
gcloud projects create vibrant-map-454012-h9 --name="Weather Pipeline"
```
*(Note: If using a different project, adjust accordingly.)*

Enable required APIs:
```bash
gcloud services enable cloudfunctions.googleapis.com run.googleapis.com cloudscheduler.googleapis.com bigquery.googleapis.com storage.googleapis.com pubsub.googleapis.com
```

Create service accounts:

  Create a service account for Cloud Run jobs:
  
  ```bash
  gcloud iam service-accounts create cloud-run-sa --display-name="Cloud Run Service Account" --project vibrant-map-454012-h9
  ```

  Create a service account for Cloud Functions:
  ``` bash
  gcloud iam service-accounts create cloud-functions-sa --display-name="Cloud Functions Service Account" --project vibrant-map-454012-h9
  ```

### 2. Cloud Storage

Create a bucket:
```bash
gsutil mb gs://weather_data_lake
```
This bucket will store the raw weather data fetched by the get-data-job.

### 3. BigQuery

Create a dataset:
```bash
bq mk --dataset vibrant-map-454012-h9:weather_data
```

Create a table with the schema:
```bash
bq mk --table vibrant-map-454012-h9:weather_data.weather_records location:STRUCT<city:STRING,localtime:TIMESTAMP>,temperature:FLOAT
```

### 4. Cloud Run Jobs

#### get-data-job
Purpose: Fetches weather data from an API and stores it in Cloud Storage.

PythonScript:
```python
from google.cloud import storage
import requests

def fetch_weather():
    api_key = open("/etc/secrets/weather-api-key").read().strip()
    url = f"http://api.openweathermap.org/data/2.5/weather?q=London&appid={api_key}"
    response = requests.get(url)
    data = response.json()
    client = storage.Client()
    bucket = client.get_bucket("weather_data_lake")
    bucket.blob("weather_data.json").upload_from_string(str(data))
```

Dockerfile:
```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY get_data.py .
CMD ["python", "get_data.py"]
```

Requirements:
```text
google-cloud-storage==2.7.0
requests==2.28.1
```

#### load-data-job
This job reads from Cloud Storage and writes to BigQuery.

```python
from google.cloud import storage
from google.cloud import bigquery

def load_to_bigquery():
    client = bigquery.Client()
    dataset_id = "weather_data"
    table_id = "weather_records"
    bucket_name = "weather_data_lake"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob("weather_data.json")
    data = blob.download_as_text()
    table = client.get_table(f"{client.project}.{dataset_id}.{table_id}")
    client.insert_rows_json(table, [eval(data)])  # Adjust parsing based on your data format
```

Dockerfile:
```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY load_data.py .
CMD ["python", "load_data.py"]
```

Requirements:
```text
google-cloud-storage==2.7.0
google-cloud-bigquery==3.10.0
```


Build and Push Docker Images:
```bash
cd load-data-job
docker build -t us-central1-docker.pkg.dev/vibrant-map-454012-h9/weather-automation-repo/load-data .
docker push us-central1-docker.pkg.dev/vibrant-map-454012-h9/weather-automation-repo/load-data
```

Deploy:
```bash
gcloud run jobs create load-data-job \
  --image us-central1-docker.pkg.dev/vibrant-map-454012-h9/weather-automation-repo/load-data \
  --region us-central1 \
  --service-account cloud-run-sa@vibrant-map-454012-h9.iam.gserviceaccount.com \
  --set-env-vars BUCKET_NAME=weather_data_lake,PROJECT_ID=vibrant-map-454012-h9,DATASET_ID=weather_data,TABLE_ID=weather_records
```

### 5.Cloud Functions

trigger-cloud-run-job
Purpose: Triggers Cloud Run jobs based on Pub/Sub messages.

Python Script:
```python
from google.cloud import run_v2
import base64

def trigger_cloud_run_job(event, context):
    """Cloud Function triggered by Pub/Sub to execute a Cloud Run Job."""
    message = base64.b64decode(event['data']).decode('utf-8')
    print(f"Received message: {message}")

    client = run_v2.JobsClient()
    project_id = "vibrant-map-454012-h9"
    region = "us-central1"

    if message == "run-get-data":
        job_name = f"projects/{project_id}/locations/{region}/jobs/get-data-job"
    elif message == "run-load-data":
        job_name = f"projects/{project_id}/locations/{region}/jobs/load-data-job"
    else:
        print(f"Unknown message: {message}")
        return

    request = run_v2.RunJobRequest(name=job_name)
    try:
        operation = client.run_job(request=request)
        print(f"Triggered job: {job_name}")
    except Exception as e:
        print(f"Failed to trigger job {job_name}: {e}")
```

Requirements:
```text
google-cloud-run==0.10.0
google-cloud-pubsub==2.18.0
```

Deploy:
```bash
cd cloud-function-trigger
gcloud functions deploy trigger-cloud-run-job \
  --runtime python310 \
  --region us-central1 \
  --trigger-topic trigger-weather-jobs \
  --project vibrant-map-454012-h9 \
  --no-gen2 \
  --entry-point trigger_cloud_run_job \
  --service-account cloud-functions-sa@vibrant-map-454012-h9.iam.gserviceaccount.com
```

### 6.Cloud Scheduler

### Create Scheduler Jobs:

Trigger get-data-job hourly:
```bash
gcloud scheduler jobs create pubsub trigger-get-data-hourly \
  --schedule="0 * * * *" \
  --topic trigger-weather-jobs \
  --message-body="run-get-data" \
  --location us-central1 \
  --project vibrant-map-454012-h9 \
  --time-zone="America/Los_Angeles"
```

Trigger load-data-job 5 minutes later:
```bash
gcloud scheduler jobs create pubsub trigger-load-data-hourly \
  --schedule="5 * * * *" \
  --topic trigger-weather-jobs \
  --message-body="run-load-data" \
  --location us-central1 \
  --project vibrant-map-454012-h9 \
  --time-zone="America/Los_Angeles"
```

### 7.IAM Permissions

### Cloud Functions Service Account:

Grant roles/run.invoker:
```bash
gcloud projects add-iam-policy-binding vibrant-map-454012-h9 \
  --member="serviceAccount:cloud-functions-sa@vibrant-map-454012-h9.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```

### Cloud Run Service Account:

Grant roles/bigquery.dataEditor and roles/storage.objectAdmin:
```bash
gcloud projects add-iam-policy-binding vibrant-map-454012-h9 \
  --member="serviceAccount:cloud-run-sa@vibrant-map-454012-h9.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding vibrant-map-454012-h9 \
  --member="serviceAccount:cloud-run-sa@vibrant-map-454012-h9.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

### Grant Dataset Access:

Export current dataset configuration:
```bash
bq show --format=prettyjson vibrant-map-454012-h9:weather_data > dataset.json
```

Edit dataset.json to add:
```json
{
  "role": "WRITER",
  "userByEmail": "627559420469-compute@developer.gserviceaccount.com"
}
```

Update the dataset:
```bash
bq update --source dataset.json vibrant-map-454012-h9:weather_data
```

## Deployment

1. Verify all components are set up:
    Cloud Storage bucket exists (gs://weather_data_lake).
    BigQuery dataset and table are created (vibrant-map-454012-h9.weather_data.weather_records).
    Cloud Run jobs are deployed.
    Cloud Function is deployed.
    Cloud Scheduler jobs are created.
2. Test the pipeline (see).

## Testing and Monitoring

### Testing

Manually Trigger Scheduler Jobs:

```bash
gcloud scheduler jobs run trigger-get-data-hourly --location us-central1 --project vibrant-map-454012-h9
gcloud scheduler jobs run trigger-load-data-hourly --location us-central1 --project vibrant-map-454012-h9
```

### Check Logs:

Cloud Run jobs:
```bash
gcloud run jobs executions describe $(gcloud run jobs executions list --job=get-data-job --region=us-central1 --project=vibrant-map-454012-h9 --format="value(metadata.name)" --limit=1) --region=us-central1 --project=vibrant-map-454012-h9
gcloud run jobs executions describe $(gcloud run jobs executions list --job=load-data-job --region=us-central1 --project=vibrant-map-454012-h9 --format="value(metadata.name)" --limit=1) --region=us-central1 --project=vibrant-map-454012-h9
```

Cloud Function:
```bash
gcloud functions logs read trigger-cloud-run-job --region us-central1 --project vibrant-map-454012-h9 --limit=50
```

### Verifying Data in BigQuery:

```sql
SELECT * FROM `vibrant-map-454012-h9.weather_data.weather_records` ORDER BY location.localtime DESC LIMIT 10;
```

## Troubleshooting

### Permission Denied:

Ensure service accounts have the correct roles (see ).
Verify dataset access (adjust dataset.json if needed).

### Cloud Function Fails:

Verify main.py is in the cloud-function-trigger directory and the entry point matches trigger_cloud_run_job.

### No Data in BigQuery:

Check gs://weather_data_lake for files:
```bash
gsutil ls gs://weather_data_lake
```

Ensure the data schema matches the BigQuery table:
```bash
bq show --schema vibrant-map-454012-h9:weather_data.weather_records
```

## Analytics

To check for analytics copy the link:

https://lookerstudio.google.com/reporting/93903903-59f1-4f92-9308-b4208c53950a
