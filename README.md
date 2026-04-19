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

### Repository Variables

- `GCP_PROJECT_ID`: tu project id.
- `GCP_REGION`: `us-central1`
- `ARTIFACT_REGISTRY_REPOSITORY`: `test-ceiba`
- `IMAGE_NAME`: `job-datos-fintrust`
- `JOB_NAME`: `job-datos-fintrust`
- `CLOUD_RUN_SERVICE_ACCOUNT`
- `GCP_DEPLOYER_SERVICE_ACCOUNT`: service account que usará GitHub Actions para desplegar.
- `GCP_WORKLOAD_IDENTITY_PROVIDER`: resource name completo del provider de Workload Identity Federation.
- `SECRET_PROJECT_ID`: project id o project number donde vive el secreto.
- `SECRET_ID`: nombre del secreto que consume el job.

## Permisos recomendados para la service account de GitHub Actions

La service account que impersona GitHub Actions debe tener como mínimo:

- `roles/run.admin`
- `roles/cloudbuild.builds.editor`
- `roles/artifactregistry.writer`
- `roles/iam.serviceAccountUser`

## Configuración recomendada de Workload Identity Federation

Si tu organización bloquea la creación de claves JSON, esta es la ruta correcta. Reemplaza `PROJECT_ID`, `PROJECT_NUMBER`, `GITHUB_ORG`, `GITHUB_REPO` y los nombres si quieres usar otros:

```bash
gcloud config set project PROJECT_ID

gcloud iam workload-identity-pools create github-pool \
  --location=global \
  --display-name="GitHub Actions Pool"

gcloud iam workload-identity-pools providers create-oidc github-provider \
  --location=global \
  --workload-identity-pool=github-pool \
  --display-name="GitHub Actions Provider" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner,attribute.ref=assertion.ref"

gcloud iam service-accounts create github-actions-deployer \
  --display-name="GitHub Actions Deployer"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:github-actions-deployer@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/run.admin"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:github-actions-deployer@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/cloudbuild.builds.editor"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:github-actions-deployer@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:github-actions-deployer@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"

gcloud iam service-accounts add-iam-policy-binding \
  github-actions-deployer@PROJECT_ID.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/github-pool/attribute.repository/GITHUB_ORG/GITHUB_REPO"
```

Después, en GitHub `Settings > Secrets and variables > Actions > Variables`, crea:

- `GCP_WORKLOAD_IDENTITY_PROVIDER`:
  `projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/github-pool/providers/github-provider`
- `GCP_DEPLOYER_SERVICE_ACCOUNT`:
  `github-actions-deployer@PROJECT_ID.iam.gserviceaccount.com`

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
