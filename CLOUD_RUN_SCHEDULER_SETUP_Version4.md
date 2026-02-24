# Deploy & Schedule PostHog + Firebase → BigQuery ETL (Cloud Run + Secret Manager + Cloud Scheduler)

This document shows a concise, well‑formatted end‑to‑end workflow to build your ETL container, deploy it to Cloud Run, provide secrets via Secret Manager, and schedule runs with Cloud Scheduler. It includes the Cloud Build logging guidance required when your org forces a user‑managed build service account.

This ETL pipeline extracts data from PostHog events/sessions and Firebase users/events, aggregates them into session-level behavioral metrics, and loads the results to BigQuery.

Use this as a checklist you can follow in the Cloud Console or Cloud Shell. Important sections are bolded; Section E (Deploy to Cloud Run) is expanded and explicit.

---

## Contents
- Overview
- Prerequisites
- Files expected in repo
- Variables (replace these)
- Checklist
- Step-by-step (A → I)
  - A — Confirm project
  - B — Create Cloud Run service account (etl-runner)
  - C — Add Firebase JSON to Secret Manager
  - D — Build the container image (Cloud Build; cloudbuild.yaml)
  - E — **Deploy to Cloud Run** (detailed)
  - F — Create scheduler service account + grant Run Invoker
  - G — Create Cloud Scheduler job (HTTP + OIDC)
  - H — Test & verify
  - I — Troubleshooting & checklist
- Extras: user-managed Cloud Build SA roles & cloudbuild.yaml
- Quick CLI snippets

---

## Overview

Goal: Containerize your ETL (includes `app.py` wrapper that calls `main.main()`), push the image to Container/Artifact Registry, deploy to Cloud Run (private), inject BigQuery credentials from Secret Manager as `BIGQUERY_CREDENTIALS_PATH`, and schedule Cloud Scheduler to POST to `/run` with OIDC authentication.

**Data Flow:**
- Extracts PostHog events & sessions from BigQuery (`posthog_etl` dataset)
- Extracts Firebase users & events from BigQuery (`firebase_etl_prod` dataset)
- Transforms data with behavioral metrics (event creation, scrolling, quiz completion, etc.)
- Loads aggregated sessions to BigQuery (`posthog_aggregated` dataset)

Why this approach:
- Reproducible environment for pandas/pyarrow/bigquery clients.
- Secret Manager secures BigQuery credentials.
- Cloud Run + Scheduler provide serverless scheduling.

---

## Prerequisites

- Project attached to a Billing Account.
- Repo contains: `Dockerfile`, `requirements.txt`, `app.py`, `main.py`, `etl_functions.py`, `process_sessions.py`.
- You have the BigQuery service account JSON locally (for Secret Manager).
- Source BigQuery datasets already exist:
  - `posthog_etl` dataset with `events` and `sessions` tables
  - `firebase_etl_prod` dataset with `users` and `events` tables
- If org requires user‑managed Cloud Build SA, create it and grant minimal roles (see Extras).

---

## Files expected in repo

- Dockerfile (already committed)
- requirements.txt (already committed)
- app.py — HTTP wrapper that handles BigQuery credentials from Secret Manager and calls `main.main()`
- main.py — ETL orchestration with parallel BigQuery queries
- etl_functions.py — Generic ETL utilities and BigQuery helpers
- process_sessions.py — Session aggregation logic with behavioral metrics
- (recommended) cloudbuild.yaml — to force Cloud Logging for builds and tag images

---

## Variables (replace when running commands or in the Console)
- PROJECT_ID = your-project-id
- REGION = us-central1
- IMAGE = gcr.io/${PROJECT_ID}/posthog-etl
- SERVICE_NAME = posthog-etl
- POSTHOG_DATASET = posthog_etl (source data)
- FIREBASE_DATASET = firebase_etl_prod (source data)
- AGGREGATED_DATASET = posthog_aggregated (output data)
- BIGQUERY_SECRET_NAME = bigquery-key
- ETL_SA = etl-runner (already created with permissions)
- BUILD_SA = cloud-build-user-sa (if required by org)
- SCHED_SA = scheduler-invoker
- SCHED_JOB_NAME = posthog-etl-trigger

---
## Checklist
One-time checklist:
- [x] Project attached to Billing
- [x] Dockerfile, requirements.txt, app.py, main.py, etl_functions.py, process_sessions.py present
- [x] (Recommended) cloudbuild.yaml added
- [x] Build image (manual or trigger) → verify image exists
- [x] ~~Create `etl-runner` SA & assign roles~~ (ALREADY DONE - reusing existing SA)
- [ ] Add `bigquery-key` secret to Secret Manager
- [ ] Deploy Cloud Run service using the built image, map secret to env var
- [ ] Verify `scheduler-invoker` SA has `roles/run.invoker` on the new service
- [ ] Create/Update Cloud Scheduler job (HTTP + OIDC) to POST to `/run`
- [ ] Run job and inspect logs

---

## Step‑by‑step

### A — Confirm project
1. Open Cloud Console: https://console.cloud.google.com  
2. Set project dropdown to `PROJECT_ID`.  
3. (Optional) Open Cloud Shell for CLI steps.

---

### B — Verify Cloud Run service account (etl‑runner)
**Purpose:** Cloud Run uses this SA to call BigQuery and Secret Manager. You mentioned this service account is already created with proper permissions from the previous ETL setup.

#### ✅ ALREADY COMPLETED - Verify existing permissions:
Your existing `etl-runner@PROJECT_ID.iam.gserviceaccount.com` should already have these roles. Double-check in IAM & Admin → IAM:

**Console Verification:**
1. **IAM & Admin** → **IAM**
2. Search for `etl-runner`
3. Verify it has the required roles below

#### Required IAM Roles for `etl-runner`

**For BigQuery Access (REQUIRED for ETL):**
- **BigQuery Data Editor** (`roles/bigquery.dataEditor`) - Read/write BigQuery table data
- **BigQuery Job User** (`roles/bigquery.jobUser`) - Run BigQuery jobs (queries, loads, exports)

**For Secret Manager (REQUIRED for Firebase credentials):**
- **Secret Manager Secret Accessor** (`roles/secretmanager.secretAccessor`) - Read secret values

**For Cloud Build (ONLY if using `etl-runner` as build service account):**
- **Logs Writer** (`roles/logging.logWriter`) - Write build logs
- **Logs Viewer** (`roles/logging.viewer`) - View build logs
- **Artifact Registry Writer** (`roles/artifactregistry.writer`) - Push Docker images
- **Artifact Registry Repository Administrator** (`roles/artifactregistry.repoAdmin`) - OPTIONAL: Allows auto-creating repositories on push

**For Firestore (OPTIONAL - only if ETL reads from Firestore):**
- **Cloud Datastore Viewer** (`roles/datastore.viewer`) - Read Firestore data

#### Assign Roles in Console:
1. **IAM & Admin** → **IAM** → **Grant Access**
2. **New principals**: `etl-runner@PROJECT_ID.iam.gserviceaccount.com`
3. **Select roles**: Add each required role from the list above
4. Click **Save**

**Important:** Before running your first build, manually create the `gcr.io` Artifact Registry repository:
1. Navigate to **Artifact Registry** → **Repositories** → **Create Repository**
2. **Name**: `gcr.io`
3. **Format**: `Docker`
4. **Location type**: `Multi-region`
5. **Region**: `us` (United States)
6. Click **Create**

This one-time step is required because organization policies may prevent auto-creation of repositories.

---

### C — Add BigQuery Credentials to Secret Manager
1. Console → Security → Secret Manager → Create secret (or update if exists).  
2. Name: `bigquery-key`  
3. Upload your BigQuery service account JSON file content.
4. Click Create.  

**Note:** The `app.py` will fetch this secret and write it to a temp file that BigQuery client can use. Alternatively, you can inject it directly as `BIGQUERY_CREDENTIALS_PATH` environment variable pointing to a mounted secret file.

---

### D — Build the container image (Cloud Build)

You can:
- Run a single manual build (fast), OR
- Create a trigger (recommended for CI). If your org requires user‑managed BUILD_SA, *add `cloudbuild.yaml`* containing `options.logging: CLOUD_LOGGING_ONLY` before creating the trigger.

Recommended `cloudbuild.yaml` (put at repo root):
```yaml
# cloudbuild.yaml
options:
  logging: CLOUD_LOGGING_ONLY

steps:
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '-t', 'gcr.io/$PROJECT_ID/posthog-etl:$SHORT_SHA', '.']

  - name: 'gcr.io/cloud-builders/docker'
    args: ['push', 'gcr.io/$PROJECT_ID/posthog-etl:$SHORT_SHA']

  - name: 'gcr.io/cloud-builders/gcloud'
    entrypoint: 'bash'
    args:
      - -c
      - |
        gcloud container images add-tag gcr.io/$PROJECT_ID/posthog-etl:$SHORT_SHA gcr.io/$PROJECT_ID/posthog-etl:latest --quiet

images:
  - 'gcr.io/$PROJECT_ID/posthog-etl:$SHORT_SHA'
  - 'gcr.io/$PROJECT_ID/posthog-etl:latest'
```

Manual build (Cloud Shell / local):
```bash
gcloud builds submit --tag gcr.io/${PROJECT_ID}/posthog-etl:latest
```

**Trigger build (Console) - Step by step:**

1. Navigate to **Cloud Build** → **Triggers** → Click **Create Trigger**

2. **Name**: Enter a descriptive name (e.g., `posthog-etl-build`)

3. **Event**: 
   - For initial setup: Choose **Manual invocation**
   - For CI/CD: Choose **Push to a branch**

4. **Source**: 
   - **Repository**: Select your connected GitHub repository (you may need to connect it first via Cloud Build → Repositories)
   - **Branch**: `^main$` (or your default branch)

5. **Configuration**:
   - **Type**: Choose **Cloud Build configuration file (yaml or json)**
   - **Location**: Repository → `/cloudbuild.yaml`

6. **Service Account**:
   - Expand **Show advanced settings**
   - Under **Service account**, select `etl-runner@PROJECT_ID.iam.gserviceaccount.com`
   - This uses the same service account for both building and running your ETL
   - (If you don't see this option, your org may allow the default Cloud Build SA)

7. Click **Create**

8. **Run the trigger**:
   - Find your trigger in the list → Click **Run** → Click **Run trigger**
   - Wait for build to complete (check **History** tab for status)

**Note:** If Cloud Build proposes an image name containing `$COMMIT_SHA`, run the build first — only after the build runs will the image with an actual SHA tag exist. For Cloud Run deployment you must pass a concrete tag (e.g., `:latest` or `:abc123`), not the literal `$COMMIT_SHA`.

---

### E — **Deploy to Cloud Run** (DETAILED; follow exactly)

This section is intentionally explicit — follow each numbered step.

1) Open: Cloud Run → **Create Service**.

2) **Region & Image**
   - Region: choose `REGION` (e.g., `us-central1`).
   - Container image: paste the concrete image path produced by Cloud Build:
     - Example: `gcr.io/PROJECT_ID/posthog-etl:latest`
     - Or: `gcr.io/PROJECT_ID/posthog-etl:<SHORT_SHA>` (copy exact tag from Cloud Build results)
   - IMPORTANT: Do NOT paste a name containing the literal `$COMMIT_SHA` variable — that image won't exist until you run the build.

3) **Service name**
   - Set: `posthog-etl`

4) **Authentication**
   - Select **Require authentication** (keep private)
   - Leave **IAM** checkbox checked (default)
   - Do NOT select "Allow public access"

5) **Service account**
   - Under Security → Service account, select:
     ```
     etl-runner@PROJECT_ID.iam.gserviceaccount.com
     ```
   - This identity is used by the container when calling Google APIs.

6) **Environment Variables** (add these in Environment Variables section):
   ```
   GOOGLE_CLOUD_PROJECT_ID = your-project-id
   POSTHOG_DATASET_ID = posthog_etl
   FIREBASE_DATASET_ID = firebase_etl_prod
   POSTHOG_AGGREGATED_DATASET_ID = posthog_aggregated
   BIGQUERY_SECRET_NAME = bigquery-key
   ```
   
   **Do NOT set `GOOGLE_APPLICATION_CREDENTIALS`** — Cloud Run uses the service account identity automatically.

7) **Container sizing & runtime**
   - Memory: start with `2Gi` for parallel BigQuery queries (increase to `4Gi` or `8Gi` if OOM).
   - Max Concurrent Requests: keep at `1` (only one scheduled ETL run should execute at a time).
   - Timeout: set sufficiently large (max 60m on Cloud Run). If ETL > 60m, consider Cloud Run Jobs.
   - CPU: `2` or `4` vCPUs recommended for parallel BigQuery operations.
   - Max instances: set to `1` to prevent overlapping ETL runs.

   **Note:** The "Max Concurrent Requests = 1" setting is for HTTP requests, NOT internal parallelization. Your ETL code can still run parallel BigQuery queries within a single request. This setting prevents multiple scheduler jobs from running simultaneously.

8) **Create / Deploy**
9) **If you see errors** **Deploy**. Wait for deployment to finish.
   - Copy the service URL (e.g., `https://posthog-etl-xxxxx-uc.a.run.app`).

10) **If you see errors**
   - "Image ... not found" — run the build and use an existing image tag (see D).
   - Permission errors pushing/pulling images — ensure build pushed to same project, or grant appropriate Artifact Registry/Storage permissions.

---

### F — Verify scheduler service account + grant Run Invoker to new service
**Note:** You likely already have `scheduler-invoker` SA from the previous ETL. You just need to grant it access to this new Cloud Run service.

**From IAM & Admin (project-level access):**
1. Navigate to **IAM & Admin** → **IAM**
2. Click **GRANT ACCESS** button
3. In **New principals**: enter `scheduler-invoker@PROJECT_ID.iam.gserviceaccount.com`
4. In **Select a role**: search for and select **Cloud Run Invoker** (`roles/run.invoker`)
5. Click **Save**

**Note:** This grants invoker access to ALL Cloud Run services in the project, which is fine for your use case.

**Option 3 - Using gcloud (if UI doesn't show the option):**
```bash
gcloud run services add-iam-policy-binding posthog-etl \
  --region=us-central1 \
  --member="serviceAccount:scheduler-invoker@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```
Replace `PROJECT_ID` with your actual project ID and adjust region if needed.

---

### G — Create/Update Cloud Scheduler job (HTTP + OIDC)
1. Cloud Scheduler → **Create Job** (or edit existing job).
2. Name: `posthog-etl-trigger`.
3. Frequency: cron expression (e.g., `0 2 * * *` for daily 02:00 UTC).
4. Target: **HTTP**.
5. URL: `<your-cloud-run-url>/run` (append `/run`).
   - To find your Cloud Run URL: Go to **Cloud Run** → click your `posthog-etl` service → copy the URL at the top of the page → append `/run` to it.
   - Example: if URL is `https://posthog-etl-abc123-uc.a.run.app`, enter `https://posthog-etl-abc123-uc.a.run.app/run`
6. HTTP method: `POST`.
7. Body (for incremental loads):
```json
{"full_load": false}
```
   Or for full reload:
```json
{"full_load": true}
```
8. **Authentication**: choose **Add OIDC token**.
   - Service account: `scheduler-invoker@PROJECT_ID.iam.gserviceaccount.com`
   - Audience: Cloud Run URL (console usually fills it)
9. Create job.

---

### H — Test & verify
- Cloud Scheduler → select job → **Run now**.
- Cloud Run → Service → Revisions → **Logs** to view stdout/stderr.
- Logging → Logs Explorer for detailed logs and errors.
- If job returns non-2xx, check Cloud Run logs for traceback.