# Weather Data Pipeline on Google Cloud

This project automates the collection and storage of weather data using Google Cloud services. The pipeline fetches weather data from an external API, stores it in Cloud Storage, and loads it into BigQuery for analysis. The process is managed using Cloud Scheduler, Cloud Functions, and Cloud Run jobs.

## Components
- **Cloud Scheduler**: Triggers the pipeline hourly.
- **Cloud Functions**: Handles Pub/Sub messages to trigger Cloud Run jobs.
- **Cloud Run Jobs**:
  - `get-data-job`: Fetches weather data from an API and stores it in Cloud Storage.
  - `load-data-job`: Loads data from Cloud Storage into BigQuery.
- **Cloud Storage**: Temporary storage for fetched data.
- **BigQuery**: Stores weather data for analysis.

## Prerequisites
- Google Cloud account with billing enabled.
- gcloud CLI installed (`gcloud auth login`).
- Docker installed for building containers.
- Weather API key (e.g., OpenWeatherMap).

## Setup Instructions
1. **Create a Google Cloud Project**
   ```bash
   gcloud projects create weather-pipeline-project --name="Weather Pipeline"
   ```
2. **Enable Required APIs**
   ```bash
   gcloud services enable cloudfunctions.googleapis.com run.googleapis.com \
   cloudscheduler.googleapis.com bigquery.googleapis.com storage.googleapis.com pubsub.googleapis.com
   ```
3. **Set Up Service Accounts**
   ```bash
   gcloud iam service-accounts create cloud-run-sa --display-name="Cloud Run Service Account"
   gcloud iam service-accounts create cloud-functions-sa --display-name="Cloud Functions Service Account"
   ```
4. **Create Cloud Storage Bucket**
   ```bash
   gsutil mb gs://weather_data_lake
   ```
5. **Set Up BigQuery Dataset and Table**
   ```bash
   bq mk --dataset weather-pipeline-project:weather_data
   bq mk --table weather-pipeline-project:weather_data.weather_records \
   location:STRUCT<city:STRING,localtime:TIMESTAMP>,temperature:FLOAT
   ```
6. **Deploy Cloud Run Jobs**
   ```bash
   docker build -t gcr.io/weather-pipeline-project/get-data .
   docker push gcr.io/weather-pipeline-project/get-data
   gcloud run jobs create get-data-job --image gcr.io/weather-pipeline-project/get-data \
   --region us-central1 --service-account cloud-run-sa@weather-pipeline-project.iam.gserviceaccount.com \
   --set-env-vars BUCKET_NAME=weather_data_lake
   ```
   Repeat for `load-data-job`.

7. **Deploy Cloud Function**
   ```bash
   gcloud functions deploy trigger-cloud-run-job --runtime python310 --region us-central1 \
   --trigger-topic trigger-weather-jobs --service-account cloud-functions-sa@weather-pipeline-project.iam.gserviceaccount.com \
   --entry-point trigger_cloud_run_job
   ```
8. **Set Up Cloud Scheduler**
   ```bash
   gcloud scheduler jobs create pubsub trigger-get-data-hourly \
   --schedule="0 * * * *" --topic trigger-weather-jobs --message-body="run-get-data" \
   --location us-central1 --time-zone="UTC"
   ```
   Repeat for `load-data-job` with a 5-minute delay.

## Testing
- Manually trigger jobs:
  ```bash
  gcloud scheduler jobs run trigger-get-data-hourly --location us-central1
  gcloud scheduler jobs run trigger-load-data-hourly --location us-central1
  ```
- Check logs:
  ```bash
  gcloud functions logs read trigger-cloud-run-job --region us-central1
  ```
- Verify data in BigQuery:
  ```sql
  SELECT * FROM `weather-pipeline-project.weather_data.weather_records` ORDER BY localtime DESC LIMIT 10;
  ```
