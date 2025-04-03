import requests
import json
import os
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account

def read_cities():
    with open("cities.txt", "r") as file:
        cities = [line.strip() for line in file if line.strip()]
    return cities

def get_weather_data(city, api_key):
    url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={city}&aqi=no"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for {city}: {response.status_code}")
        return None

def upload_to_gcs(city, data, bucket_name):
    credentials = service_account.Credentials.from_service_account_file(
        "/etc/secrets/service-account-key.json",
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    blob_name = f"weather_data/{city}/{city}_{timestamp}.json"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    print(f"Uploaded {blob_name} to GCS")

def fetch_weather_data():
    """Fetches weather data and uploads to GCS."""
    # Load sensitive information from environment variables
    api_key = os.getenv("WEATHER_API_KEY")
    bucket_name = os.getenv("BUCKET_NAME")

    if not all([api_key, bucket_name]):
        raise ValueError("Missing required environment variables: WEATHER_API_KEY or BUCKET_NAME")

    cities = read_cities()
    print(f"Fetching data for cities: {cities}")
    for city in cities:
        data = get_weather_data(city, api_key)
        if data:
            try:
                upload_to_gcs(city, data, bucket_name)
            except Exception as e:
                print(f"Failed to upload data for {city}: {e}")

if __name__ == "__main__":
    fetch_weather_data()