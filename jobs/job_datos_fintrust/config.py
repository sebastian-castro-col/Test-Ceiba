import json
import os
from google.cloud import secretmanager


PROJECT_ID = os.getenv("APP_PROJECT_ID", "")
SECRET_PROJECT_ID = os.getenv("SECRET_PROJECT_ID", "")
SECRET_ID = os.getenv("SECRET_ID", "")


def get_credentials():
    if not SECRET_PROJECT_ID or not SECRET_ID:
        raise ValueError(
            "SECRET_PROJECT_ID y SECRET_ID deben estar configurados como variables de entorno."
        )

    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{SECRET_PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"

    response = client.access_secret_version(name=secret_name)
    payload = response.payload.data.decode("UTF-8").strip()

    if not payload:
        raise ValueError(f"El secreto {SECRET_ID} no contiene datos.")

    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return payload
