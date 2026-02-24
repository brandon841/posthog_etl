# Minimal ETL Pipeline Template

This is a stripped-down ETL (Extract-Transform-Load) pipeline framework. It provides the core structure for scheduling, running, and monitoring ETL tasks, with stub modules ready to expand for your actual data and connectors.

## Structure

- `main.py`: Pipeline entry point.
- `etl_functions.py`: Framework for generic Extract, Transform, Load operations.
- `process_template.py`: Example module showing how to implement a new ETL step.
- `app.py`: (Optional) App runner or API integration point.
- `requirements.txt`, `Dockerfile`, `cloudbuild.yaml`: For deployment & builds.
- `Testing/`: Placeholder for your test code and test data.

## TO DO 
* Update date_filter in main.py for incremental loading
* See if we can calculate scroll depth using touch_x and touch_y from posthog events
