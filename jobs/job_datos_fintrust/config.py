import json
from google.cloud import secretmanager


PROJECT_ID = ''


def get_credentials():
    client = secretmanager.SecretManagerServiceClient()

    project_id = '31340601742'
    secret_id = 'Test-Ceiba-github-oauthtoken-35ef2c'

    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    response = client.access_secret_version(name=secret_name)
    payload = response.payload.data.decode("UTF-8")

    return json.loads(payload)

