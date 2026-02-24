# PostHog + Firebase → BigQuery ETL Pipeline

## TO DO 
* Update date_filter in main.py for incremental loading
* See if we can calculate scroll depth using touch_x and touch_y from posthog events
* Create aggregated person table from aggregated session table

---

Production-grade ETL pipeline that extracts behavioral data from PostHog events/sessions and Firebase users/events, transforms it with session-level aggregations, and loads results into BigQuery for analytics.

## Architecture

```
PostHog Events & Sessions (BigQuery)  ─┐
                                        ├─→ ETL Processing ─→ BigQuery Aggregated Dataset
Firebase Users & Events (BigQuery)    ─┘
```

**Data Flow:**
- **Extract**: Parallel queries from PostHog and Firebase BigQuery datasets
- **Transform**: Session-level behavioral metrics (event creation, scrolling, quiz completion, etc.)
- **Load**: Aggregated sessions to `posthog_aggregated` BigQuery dataset

**Deployment:**
- Containerized with Docker
- Deployed to Cloud Run (private, authenticated)
- Scheduled with Cloud Scheduler (daily runs)
- Secrets managed via Secret Manager
- CI/CD via Cloud Build triggers

---

## Features

✅ **Incremental Loading** - Only processes new data since last run  
✅ **Parallel BigQuery Queries** - Fetches all tables simultaneously  
✅ **Production-Ready** - Gunicorn WSGI server with proper error handling  
✅ **Secure** - Credentials via Secret Manager, private Cloud Run endpoint  
✅ **Automated Deployment** - Push to main → auto-build → auto-deploy  
✅ **Flexible Scheduling** - Cron-based Cloud Scheduler with full/incremental modes  

---

## Prerequisites

- Google Cloud Project with billing enabled
- BigQuery datasets:
  - `posthog_etl` - PostHog events and sessions
  - `firebase_etl_prod` - Firebase users and events
  - `posthog_aggregated` - Output dataset (auto-created)
- Service accounts:
  - `etl-runner` - For BigQuery/Secret Manager access
  - `scheduler-invoker` - For Cloud Scheduler authentication
- Secret Manager secret:
  - `bigquery-key` - BigQuery service account credentials

---

## Project Structure

```
posthog_etl/
├── main.py                 # ETL orchestration & entry point
├── etl_functions.py        # Generic ETL utilities & BigQuery helpers
├── process_sessions.py     # Session aggregation logic
├── app.py                  # Flask HTTP wrapper for Cloud Run
├── requirements.txt        # Python dependencies
├── Dockerfile             # Container build configuration
├── cloudbuild.yaml        # CI/CD build & deployment pipeline
└── CLOUD_RUN_SCHEDULER_SETUP_Version4.md  # Detailed setup guide
```

---

## Environment Variables

### Required
- `GOOGLE_CLOUD_PROJECT_ID` - GCP project ID
- `POSTHOG_DATASET_ID` - PostHog BigQuery dataset (default: `posthog_etl`)
- `FIREBASE_DATASET_ID` - Firebase BigQuery dataset (default: `firebase_etl_prod`)
- `POSTHOG_AGGREGATED_DATASET_ID` - Output dataset name
- `BIGQUERY_SECRET_NAME` - Secret Manager secret name (default: `bigquery-key`)

### Optional (Local Development)
- `BIGQUERY_CREDENTIALS_PATH` - Path to BigQuery credentials JSON

---

## Quick Start

### Local Development

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up credentials:**
   ```bash
   export GOOGLE_CLOUD_PROJECT_ID="your-project-id"
   export BIGQUERY_CREDENTIALS_PATH="path/to/credentials.json"
   export POSTHOG_DATASET_ID="posthog_etl"
   export FIREBASE_DATASET_ID="firebase_etl_prod"
   export POSTHOG_AGGREGATED_DATASET_ID="posthog_aggregated"
   ```

3. **Run ETL:**
   ```bash
   # Incremental load
   python main.py
   
   # Full load
   python main.py --full-load
   
   # Test with limited records
   python main.py --limit 100
   ```

### Test Flask API Locally

```bash
# Start server
python app.py

# Trigger ETL
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{"full_load": false}'
```

---

## Deployment

### Initial Setup

Follow the comprehensive guide in `CLOUD_RUN_SCHEDULER_SETUP_Version4.md` which covers:
- Service account creation and permissions
- Secret Manager configuration
- Cloud Build trigger setup
- Cloud Run deployment
- Cloud Scheduler configuration

### CI/CD Pipeline

**Automated on push to `main` branch:**

1. **Build** - Docker image built with all dependencies
2. **Push** - Image pushed to Artifact Registry as `gcr.io/PROJECT_ID/posthog-etl:latest`
3. **Deploy** - New Cloud Run revision deployed automatically

**Manual build trigger:**
```bash
# Via gcloud
gcloud builds submit --tag gcr.io/PROJECT_ID/posthog-etl:latest

# Via Console
Cloud Build → Triggers → posthog-etl-build → Run
```

### Cloud Run Configuration

- **Service**: `posthog-etl`
- **Region**: `us-central1`
- **Memory**: 2Gi (adjustable for larger datasets)
- **CPU**: 2 vCPUs
- **Timeout**: 60 minutes
- **Concurrency**: 1 (prevents overlapping runs)
- **Max Instances**: 1
- **Authentication**: Required (private endpoint)

---

## Usage

### Scheduled Runs (Cloud Scheduler)

**Daily incremental load** (default):
```json
POST /run
{
  "full_load": false
}
```

**Full reload** (reprocess all data):
```json
POST /run
{
  "full_load": true
}
```

### Manual Trigger

**Via Cloud Scheduler:**
```
Cloud Scheduler → posthog-etl-trigger → Run Now
```

**Via gcloud:**
```bash
gcloud scheduler jobs run posthog-etl-trigger --location=us-central1
```

**Via curl (requires authentication):**
```bash
curl -X POST https://posthog-etl-XXXXX.a.run.app/run \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{"full_load": false}'
```

---

## Monitoring

### Cloud Run Logs
```
Cloud Run → posthog-etl → Logs
```

View real-time ETL progress:
- Data extraction timing
- Record counts
- Processing status
- Error tracebacks

### Cloud Scheduler Status
```
Cloud Scheduler → posthog-etl-trigger → Execution history
```

### BigQuery Output
```sql
SELECT COUNT(*) FROM `PROJECT_ID.posthog_aggregated.sessions`
WHERE _loaded_at >= CURRENT_DATE()
```

---

## Development Workflow

1. **Create feature branch:**
   ```bash
   git checkout -b feature/new-metric
   ```

2. **Make changes** to `process_sessions.py` or other modules

3. **Test locally:**
   ```bash
   python main.py --limit 10
   ```

4. **Commit and push:**
   ```bash
   git add .
   git commit -m "Add new behavioral metric"
   git push origin feature/new-metric
   ```

5. **Merge to main** - Triggers automatic build and deployment

---

## Troubleshooting

### No logs appearing in Cloud Run
- Check `PYTHONUNBUFFERED=1` is set in Dockerfile
- Verify gunicorn is running (not Flask dev server)

### Permission denied errors
- Verify `etl-runner` service account has required roles:
  - BigQuery Data Editor
  - BigQuery Job User
  - Secret Manager Secret Accessor

### Cloud Build deployment failed
- Check that branch is `main` (deployments only run on main)
- Verify service account has Cloud Run Admin role for deployments

### ETL timeout
- Increase Cloud Run timeout (max 60 minutes)
- Consider switching to Cloud Run Jobs for longer-running ETLs
- Optimize BigQuery queries or add pagination

### Missing data in output
- Check date filters in `main.py`
- Verify source tables contain expected data
- Review ETL logs for processing errors

---

## Contributing

When adding new ETL processes:

1. Create new module in project root (e.g., `process_new_metric.py`)
2. Follow pattern from `process_sessions.py`:
   - Accept DataFrames as input
   - Return processed DataFrame
   - Include BigQuery client parameters
3. Add to `main.py` parallel queries
4. Update this README with new features

---

## License

Internal use - HeyYall Production Data Pipeline

---

## Support

For setup issues, see `CLOUD_RUN_SCHEDULER_SETUP_Version4.md`  
For ETL logic questions, review `process_sessions.py` inline documentation
