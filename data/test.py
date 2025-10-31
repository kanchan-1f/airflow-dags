from google.cloud import secretmanager
import json
import os

def get_service_account_key(secret_name, project_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    return json.loads(response.payload.data.decode("UTF-8"))

key_data = get_service_account_key("my-sa-key", "my-gcp-project")
os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"] = json.dumps(key_data)

secret_name ='analytics_1f_sk'
project_id = 'analytics-1f'
get_service_account_key(secret_name=secret_name, project_id=project_id)