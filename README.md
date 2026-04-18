# Test-Ceiba

Despliegue automatizado de `job_datos_fintrust` en Cloud Run Jobs usando GitHub Actions.

## Qué quedó listo en el repo

- Workflow real en `.github/workflows/deploy.yml`.
- Build del contenedor usando `jobs/job_datos_fintrust` como contexto.
- Despliegue del job `job-datos-fintrust` a Cloud Run Jobs.
- Variables del job parametrizadas por entorno en vez de usar IDs quemados.
- `cloudbuild.yaml` alineado con la misma estrategia para despliegues manuales.

## Recursos que debes crear una sola vez en GCP

Asumo:

- Región: `us-central1`
- Artifact Registry: `test-ceiba`
- Job: `job-datos-fintrust`
- Cuenta de servicio del job: `cloud-run-jobs@PROJECT_ID.iam.gserviceaccount.com`

Reemplaza `PROJECT_ID`, `PROJECT_NUMBER` y los valores del secreto antes de ejecutar:

```bash
gcloud config set project PROJECT_ID

gcloud services enable \
  artifactregistry.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  iam.googleapis.com

gcloud artifacts repositories create test-ceiba \
  --repository-format=docker \
  --location=us-central1 \
  --description="Docker images for Test-Ceiba jobs"

gcloud iam service-accounts create cloud-run-jobs \
  --display-name="Cloud Run Jobs Service Account"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:cloud-run-jobs@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:cloud-run-jobs@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:cloud-run-jobs@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"
```

Si el query del job escribe tablas en BigQuery, agrega también:

```bash
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:cloud-run-jobs@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
```

## Secretos y variables en GitHub

En tu repo de GitHub crea:

### Secret

- `GCP_CREDENTIALS`: JSON de una service account con permisos para `Cloud Build`, `Artifact Registry`, `Cloud Run Admin` y `Service Account User`.

### Repository Variables

- `GCP_PROJECT_ID`: tu project id.
- `GCP_REGION`: `us-central1`
- `ARTIFACT_REGISTRY_REPOSITORY`: `test-ceiba`
- `IMAGE_NAME`: `job-datos-fintrust`
- `JOB_NAME`: `job-datos-fintrust`
- `CLOUD_RUN_SERVICE_ACCOUNT`: `cloud-run-jobs@PROJECT_ID.iam.gserviceaccount.com`
- `SECRET_PROJECT_ID`: project id o project number donde vive el secreto.
- `SECRET_ID`: nombre del secreto que consume el job.

## Permisos recomendados para la service account de GitHub Actions

La cuenta del JSON de `GCP_CREDENTIALS` debe tener como mínimo:

- `roles/run.admin`
- `roles/cloudbuild.builds.editor`
- `roles/artifactregistry.writer`
- `roles/iam.serviceAccountUser`

## Cómo disparar el despliegue

- Push a `main`, o
- ejecútalo manualmente desde la pestaña Actions con `workflow_dispatch`.

## Despliegue manual opcional con Cloud Build

También puedes desplegar sin GitHub Actions:

```bash
gcloud builds submit --config cloudbuild.yaml \
  --substitutions=_REGION=us-central1,_REPOSITORY=test-ceiba,_IMAGE=job-datos-fintrust,_JOB_NAME=job-datos-fintrust,_SERVICE_ACCOUNT=cloud-run-jobs@PROJECT_ID.iam.gserviceaccount.com,_SECRET_PROJECT_ID=SECRET_PROJECT_ID,_SECRET_ID=SECRET_ID
```

## Importante antes de correr en producción

El query de `jobs/job_datos_fintrust/transformations/customers.py` sigue apuntando a `proyecto.dataset.tabla`. Debes reemplazarlo por la consulta real de Fintrust antes de ejecutar el job en producción.
