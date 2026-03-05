# PostHog + Firebase → BigQuery ETL Pipeline

## TO DO 
* Update date_filter in main.py for incremental loading
* See if we can calculate scroll depth using touch_x and touch_y from posthog events

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
├── main.py                     # ETL orchestration & entry point
├── etl_functions.py            # Generic ETL utilities & BigQuery helpers
├── process_sessions.py         # Session aggregation logic
├── process_people.py           # User-level aggregation from sessions
├── process_daily_activity.py   # Daily user activity metrics
├── process_churn.py            # User churn state calculation
├── app.py                      # Flask HTTP wrapper for Cloud Run
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Container build configuration
├── cloudbuild.yaml             # CI/CD build & deployment pipeline
└── CLOUD_RUN_SCHEDULER_SETUP_Version4.md  # Detailed setup guide
```

---

## Output Tables

The ETL pipeline creates four main tables in the `posthog_aggregated` BigQuery dataset:

### 1. `sessions_aggregated`
Session-level behavioral metrics and user metadata.

**Columns:**
- `session_id` - Unique session identifier (required)
- `distinct_id` - PostHog distinct user identifier
- `user_id` - Firebase user ID
- `city` - User's geographic city
- `country` - User's geographic country
- `start_timestamp` - Session start time
- `end_timestamp` - Session end time
- `session_duration` - Length of session in seconds
- `created_event` - Boolean flag: user created an event during session
- `viewed_event` - Boolean flag: user viewed an event
- `joined_event` - Boolean flag: user joined an event
- `invited_someone` - Boolean flag: user sent an invite
- `enabled_contacts` - Boolean flag: user enabled contact access
- `scrolled` - Boolean flag: user scrolled during session
- `visited_discover` - Boolean flag: user visited Discover page
- `started_quiz` - Boolean flag: user started the quiz
- `completed_quiz` - Boolean flag: user completed the quiz
- `scroll_event_count` - Number of scroll events in session
- `autocapture_count` - Number of autocapture events
- `screen_count` - Number of unique screens visited
- `fullName` - User's full name
- `phoneNumber` - User's phone number
- `username` - User's username
- `email` - User's email address
- `contactAccessGranted` - Boolean: contact access permission status
- `businessUser` - Boolean flag: business user status
- `createdAt` - User account creation timestamp
- `etl_loaded_at` - ETL processing timestamp

### 2. `people_aggregated`
User-level aggregated metrics summarizing all sessions per user.

**Columns:**
- `user_id` - Firebase user ID (required)
- `total_sessions` - Total number of sessions for user
- `avg_session_duration` - Average session length in seconds
- `median_session_duration` - Median session length in seconds
- `total_session_duration` - Combined duration of all sessions
- `min_session_duration` - Shortest session duration
- `max_session_duration` - Longest session duration
- `std_session_duration` - Standard deviation of session durations
- `created_event_sum` - Total number of sessions where user created events
- `viewed_event_sum` - Total number of sessions where user viewed events
- `joined_event_sum` - Total number of sessions where user joined events
- `invited_someone_sum` - Total number of sessions where user invited someone
- `enabled_contacts_sum` - Total number of sessions where user enabled contacts
- `scrolled_sum` - Total number of sessions where user scrolled
- `visited_discover_sum` - Total number of sessions where user visited Discover
- `started_quiz_sum` - Total number of sessions where user started quiz
- `completed_quiz_sum` - Total number of sessions where user completed quiz
- `total_scrolls` - Total scroll events across all sessions
- `avg_scrolls_per_session` - Average scroll events per session
- `max_scrolls_per_session` - Maximum scrolls in a single session
- `total_autocaptures` - Total autocapture events across all sessions
- `avg_autocaptures_per_session` - Average autocapture events per session
- `max_autocaptures_per_session` - Maximum autocaptures in a single session
- `total_screens` - Total unique screens viewed across all sessions
- `avg_screens_per_session` - Average screens per session
- `max_screens_per_session` - Maximum screens in a single session
- `first_session_date` - Timestamp of user's first session
- `last_session_date` - Timestamp of user's most recent session
- `days_since_first_session` - Number of days between first and last session
- `sessions_per_day` - Average sessions per day since first session
- `engagement_score` - Percentage of sessions with key activity like created/joined/viewed an event or scrolled
- `fullName` - User's full name
- `phoneNumber` - User's phone number
- `username` - User's username
- `email` - User's email address
- `contactAccessGranted` - Boolean: contact access permission status
- `businessUser` - Boolean flag: business user status
- `createdAt` - User account creation timestamp
- `city` - User's geographic city
- `country` - User's geographic country
- `etl_loaded_at` - ETL processing timestamp

### 3. `user_daily_activity`
Daily activity metrics per user showing engagement patterns over time.

**Columns:**
- `user_id` - Firebase user ID (required)
- `date` - Activity date (required)
- `event_count` - Number of PostHog events on this date
- `events_created_count` - Number of Firebase events created
- `accepted` - Number of event invites accepted
- `invited` - Number of invites sent
- `rejected` - Number of event invites rejected
- `createdAt` - User account creation timestamp
- `phoneNumber` - User's phone number
- `fullName` - User's full name
- `username` - User's username
- `email` - User's email address
- `etl_loaded_at` - ETL processing timestamp

### 4. `user_churn_state`
Current churn status tracking app activity and business activity separately.

**Columns:**
- `user_id` - Firebase user ID (required)
- `app_churn_state` - Churn state for app usage: active, churned, reactivated, or never_active
- `app_churn_date` - Date when user churned from app
- `app_times_churned` - Number of times user has churned and returned (app)
- `days_since_last_app_activity` - Days since last app interaction
- `first_app_active_date` - Date of first app activity
- `last_app_active_date` - Date of most recent app activity
- `biz_churn_state` - Churn state for business activity: active, churned, reactivated, or never_active
- `biz_churn_date` - Date when user churned from business activity
- `biz_times_churned` - Number of times user has churned and returned (business)
- `days_since_last_biz_activity` - Days since last business activity
- `first_biz_active_date` - Date of first business activity
- `last_biz_active_date` - Date of most recent business activity
- `total_events_created` - Lifetime count of events created
- `total_events_attended` - Lifetime count of events attended
- `total_app_interactions` - Lifetime count of app interactions
- `etl_loaded_at` - ETL processing timestamp

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
2. Follow pattern from existing process modules (`process_sessions.py`, `process_people.py`, `process_daily_activity.py`, `process_churn.py`):
   - Define schema using `define_schema_*()` function with BigQuery SchemaFields
   - Accept DataFrames as input
   - Return processed DataFrame
   - Include BigQuery client parameters for data loading
   - Use `ensure_required_columns()` and `load_dataframe_to_bigquery()` from `etl_functions.py`
3. Add to `main.py` orchestration
4. Update this README with new table schema and column descriptions

---

## License

Internal use - HeyYall Production Data Pipeline

---

## Support

For setup issues, see `CLOUD_RUN_SCHEDULER_SETUP_Version4.md`  
For ETL logic questions, review inline documentation in process modules:
- `process_sessions.py` - Session aggregation
- `process_people.py` - User-level metrics  
- `process_daily_activity.py` - Daily activity tracking
- `process_churn.py` - Churn state calculation
