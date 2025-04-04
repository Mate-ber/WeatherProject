from google.cloud import run_v2
from google.cloud import pubsub_v1
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
