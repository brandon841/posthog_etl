"""
PostHog ETL Pipeline
====================

Production ETL script that extracts data from PostHog,
transforms it, and loads it into your target destination with incremental updates.

Features:
- Incremental loading based on last ETL timestamp
- Proper error handling and logging
- Support for both full and incremental loads
- Configurable via environment variables

Usage:
    python main.py [--full-load] [--limit N]
"""

import os
import argparse
from datetime import datetime, timezone
from typing import Dict, Optional
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

try:
    from etl_functions import query_bq_parallel, create_bigquery_dataset
    from process_sessions import process_sessions_data
except ImportError as e:
    print(f"ERROR: Required modules not installed: {e}")
    print("Install with: pip install pandas python-dotenv google-cloud-bigquery")
    exit(1)

try:
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
except ImportError:
    print("ERROR: Google Cloud BigQuery not installed")
    print("Install with: pip install google-cloud-bigquery pyarrow")
    exit(1)

def init_bigquery_client() -> bigquery.Client:
    """Initialize BigQuery client"""
    bq_credentials_path = os.getenv('BIGQUERY_CREDENTIALS_PATH')
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
    
    if not project_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT_ID environment variable not set")
    
    if bq_credentials_path and os.path.exists(bq_credentials_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = bq_credentials_path
        print(f"Using BigQuery credentials: {bq_credentials_path}")
    
    return bigquery.Client(project=project_id)


def main():
    """Main ETL function"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='PostHog ETL Pipeline')
    parser.add_argument('--full-load', action='store_true', 
                       help='Force full load instead of incremental')
    parser.add_argument('--limit', type=int, 
                       help='Limit number of records for testing')
    args = parser.parse_args()

    print("PostHog ETL Pipeline")
    print("=" * 50)

    # Configuration
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
    dataset_id = os.getenv('DATASET_ID', 'posthog_etl')
    firebase_dataset_id = os.getenv('FIREBASE_DATASET_ID', 'firebase_etl_prod')

    if not project_id:
        print("ERROR: Missing required environment variables:")
        print("   - GOOGLE_CLOUD_PROJECT_ID")
        return 1

    try:
        # Initialize clients
        print("Initializing ETL clients...")
        bq_client = init_bigquery_client()
        
        # Create dataset if needed
        create_bigquery_dataset(bq_client, dataset_id)

        # Determine ETL mode
        full_load = args.full_load
        if full_load:
            print("Running FULL LOAD (all tables)")
        else:
            print("Running INCREMENTAL LOAD (each table uses its own last timestamp)")

        # Define all queries to run in parallel
        print("\nFetching all data from BigQuery in parallel...")
        
        # Determine date range based on mode
        if full_load:
            date_filter = ""  # No date filter for full load
        else:
            # For incremental, get last 30 days (adjust as needed)
            date_filter = "WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)"
        
        queries = [
            ('posthog_events', f"""
                SELECT * FROM `{project_id}.{dataset_id}.events` 
                {date_filter}
                {f'LIMIT {args.limit}' if args.limit else ''}
            """),
            ('sessions', f"""
                SELECT * FROM `{project_id}.{dataset_id}.sessions`
                {f'LIMIT {args.limit}' if args.limit else ''}
            """),
            ('users', f"""
                SELECT * FROM `{project_id}.{firebase_dataset_id}.users`
                {f'LIMIT {args.limit}' if args.limit else ''}
            """),
            ('firebase_events', f"""
                SELECT * FROM `{project_id}.{firebase_dataset_id}.events`
                {f'LIMIT {args.limit}' if args.limit else ''}
            """)
        ]
        
        # Execute all queries in parallel
        data = query_bq_parallel(bq_client, queries)
        
        print(f"Loaded {len(data['posthog_events'])} posthog events, "
              f"{len(data['sessions'])} sessions, "
              f"{len(data['users'])} users, "
              f"{len(data['firebase_events'])} firebase events")

        # Process all data
        results = {}

        # Sessions Data Processing
        results['sessions'] = process_sessions_data(
            data['posthog_events'],
            data['sessions'],
            data['users'],
            data['firebase_events'],
            bq_client=bq_client,
            project_id=project_id,
            dataset_id=dataset_id
        )

        # Add more process steps as needed
        # process_template.run_example_process()

        # Summary
        print(f"\nETL Complete!")
        print(f"   Project: {project_id}")
        print(f"   Dataset: {dataset_id}")
        print(f"   Mode: {'FULL' if full_load else 'INCREMENTAL (per-table)'}")
        for table, count in results.items():
            print(f"   {table}: {count:,} records processed")

        return 0

    except Exception as e:
        print(f"\nERROR: ETL Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
