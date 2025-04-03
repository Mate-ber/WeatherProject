import requests
from google.cloud import storage
from google.oauth2 import service_account
import datetime

# WeatherAPI key
API_KEY = "d7ca8805fba7426694894835250304"

# Google Cloud Storage bucket name
BUCKET_NAME = "weather_data_lake"

# Path to the service account key file
SERVICE_ACCOUNT_KEY_PATH = "../vibrant-map-454012-h9-051c9c818db3.json"

def read_cities():
    with open("../load-data/cities.txt", "r") as file:
        cities = [line.strip() for line in file if line.strip()]
    return cities

def fetch_and_store_weather():
    """
    Fetches current weather data from WeatherAPI for specified cities and uploads it to a GCS bucket.
    """
    # Read cities from the file
    CITIES = read_cities()
    print(f"Cities to process: {CITIES}")

    # Authenticate with the service account
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_KEY_PATH,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = storage.Client(credentials=credentials, project=credentials.project_id)
        bucket = client.get_bucket(BUCKET_NAME)
        print(f"Successfully connected to bucket: {BUCKET_NAME}")
    except Exception as e:
        print(f"Failed to connect to bucket {BUCKET_NAME}: {e}")
        return

    for city in CITIES:
        # Construct the WeatherAPI URL with the API key and city
        url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={city}"

        try:
            # Fetch the JSON data from WeatherAPI
            response = requests.get(url)
            response.raise_for_status()  # Raise an error if the request fails
            data = response.json()

            # Generate a unique timestamp for the file
            timestamp = datetime.datetime.utcnow().isoformat()

            # Define the file path in the GCS bucket
            blob_path = f"weather_data/{city}/{timestamp}.json"

            # Upload the JSON data to GCS
            blob = bucket.blob(blob_path)
            blob.upload_from_string(str(data), content_type="application/json")

            print(f"Successfully uploaded weather data for {city} to gs://{BUCKET_NAME}/{blob_path}")

        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data for {city}: {e}")
        except Exception as e:
            print(f"Failed to upload data for {city} to GCS: {e}")

if __name__ == "__main__":
    fetch_and_store_weather()