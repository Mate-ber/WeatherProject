# Weather Data Pipeline on Google Cloud

This project automates the collection and storage of weather data using Google Cloud services. The pipeline fetches weather data from an external API, stores it in Cloud Storage, and loads it into BigQuery for analysis. The process is orchestrated using Cloud Scheduler, Cloud Functions, and Cloud Run jobs.

## Table of Contents
- [Project Overview](#project-overview)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Deployment](#deployment)
- [Testing and Monitoring](#testing-and-monitoring)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Project Overview
The pipeline consists of the following components:
- **Cloud Scheduler**: Triggers the pipeline hourly.
- **Pub/Sub**: Manages messaging between components.
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
Create a Google Cloud Project (if not already created):
```bash
gcloud projects create vibrant-map-454012-h9 --name="Weather Pipeline"
```
Enable Required APIs:
```bash
gcloud services enable cloudfunctions.googleapis.com run.googleapis.com cloudscheduler.googleapis.com bigquery.googleapis.com storage.googleapis.com pubsub.googleapis.com
```

Set Up Service Accounts:
```bash
gcloud iam service-accounts create cloud-run-sa --display-name="Cloud Run Service Account" --project vibrant-map-454012-h9
gcloud iam service-accounts create cloud-functions-sa --display-name="Cloud Functions Service Account" --project vibrant-map-454012-h9
```

### 2. Pub/Sub Setup
Create a Pub/Sub topic and subscriptions:
```bash
gcloud pubsub topics create trigger-weather-jobs
```
Create subscriptions:
```bash
gcloud pubsub subscriptions create trigger-get-data-sub --topic trigger-weather-jobs
gcloud pubsub subscriptions create trigger-load-data-sub --topic trigger-weather-jobs
```

### 3. Cloud Storage
Create a Bucket:
```bash
gsutil mb gs://weather_data_lake
```

### 4. BigQuery
Create a Dataset and Table:
```bash
bq mk --dataset vibrant-map-454012-h9:weather_data
bq mk --table vibrant-map-454012-h9:weather_data.weather_records city:STRING,localtime:TIMESTAMP,temperature:FLOAT
```

### 5. Cloud Run Jobs
#### 5.1. `get-data-job`
Build and push the image:
```bash
docker build -t us-central1-docker.pkg.dev/vibrant-map-454012-h9/weather-automation-repo/get-data .
docker push us-central1-docker.pkg.dev/vibrant-map-454012-h9/weather-automation-repo/get-data
```
Deploy the job:
```bash
gcloud run jobs create get-data-job \
  --image us-central1-docker.pkg.dev/vibrant-map-454012-h9/weather-automation-repo/get-data \
  --region us-central1 \
  --service-account cloud-run-sa@vibrant-map-454012-h9.iam.gserviceaccount.com \
  --set-env-vars BUCKET_NAME=weather_data_lake \
  --set-secrets WEATHER_API_KEY=weather-api-key:latest
```

#### 5.2. `load-data-job`
Build and push the image:
```bash
docker build -t us-central1-docker.pkg.dev/vibrant-map-454012-h9/weather-automation-repo/load-data .
docker push us-central1-docker.pkg.dev/vibrant-map-454012-h9/weather-automation-repo/load-data
```
Deploy the job:
```bash
gcloud run jobs create load-data-job \
  --image us-central1-docker.pkg.dev/vibrant-map-454012-h9/weather-automation-repo/load-data \
  --region us-central1 \
  --service-account cloud-run-sa@vibrant-map-454012-h9.iam.gserviceaccount.com \
  --set-env-vars BUCKET_NAME=weather_data_lake,PROJECT_ID=vibrant-map-454012-h9,DATASET_ID=weather_data,TABLE_ID=weather_records
```

### 6. Cloud Functions
Deploy Cloud Function to trigger Cloud Run jobs:
```bash
gcloud functions deploy trigger-cloud-run-job \
  --runtime python310 \
  --region us-central1 \
  --trigger-topic trigger-weather-jobs \
  --project vibrant-map-454012-h9 \
  --entry-point trigger_cloud_run_job \
  --service-account cloud-functions-sa@vibrant-map-454012-h9.iam.gserviceaccount.com
```

### 7. Cloud Scheduler
Schedule job executions:
```bash
gcloud scheduler jobs create pubsub trigger-get-data-hourly \
  --schedule="0 * * * *" \
  --topic trigger-weather-jobs \
  --message-body="run-get-data" \
  --location us-central1 \
  --project vibrant-map-454012-h9
```
```bash
gcloud scheduler jobs create pubsub trigger-load-data-hourly \
  --schedule="5 * * * *" \
  --topic trigger-weather-jobs \
  --message-body="run-load-data" \
  --location us-central1 \
  --project vibrant-map-454012-h9
```

## Deployment
Verify all components are set up:
- Pub/Sub topics and subscriptions.
- Cloud Storage bucket (`gs://weather_data_lake`).
- BigQuery dataset and table (`vibrant-map-454012-h9.weather_data.weather_records`).
- Cloud Run jobs deployed.
- Cloud Function deployed.
- Cloud Scheduler jobs created.

## Testing and Monitoring
### Testing
Trigger jobs manually:
```bash
gcloud scheduler jobs run trigger-get-data-hourly --location us-central1 --project vibrant-map-454012-h9
gcloud scheduler jobs run trigger-load-data-hourly --location us-central1 --project vibrant-map-454012-h9
```
Verify data in BigQuery:
```sql
SELECT * FROM `vibrant-map-454012-h9.weather_data.weather_records` ORDER BY localtime DESC LIMIT 10;
```

### Monitoring
Set up Google Cloud Monitoring alerts for:
- Cloud Run job failures.
- Data freshness in BigQuery:
```sql
SELECT COUNT(*) FROM `vibrant-map-454012-h9.weather_data.weather_records` WHERE localtime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```

## Troubleshooting
### Permission Denied
Ensure service accounts have the correct roles:
```bash
gcloud projects add-iam-policy-binding vibrant-map-454012-h9 --member="serviceAccount:cloud-run-sa@vibrant-map-454012-h9.iam.gserviceaccount.com" --role="roles/bigquery.dataEditor"
```

### No Data in BigQuery
Check Cloud Storage for files:
```bash
gsutil ls gs://weather_data_lake
```

