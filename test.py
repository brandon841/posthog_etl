from google.cloud import bigquery
import os
import pandas as pd
import numpy as np
import json

from plotnine import *

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

#init BigQuery client
bq_client = init_bigquery_client()
# Read from the 'sessions_aggregated' table in BigQuery


query = """
    SELECT * FROM `etl-testing-478716.posthog_etl.events`
"""
posthog_events_df = bq_client.query(query).to_dataframe()

query = """
    SELECT * FROM `etl-testing-478716.firebase_etl_prod.users`
"""
users_df = bq_client.query(query).to_dataframe()

query = """
    SELECT * FROM `etl-testing-478716.firebase_etl_prod.events`
"""
events_df = bq_client.query(query).to_dataframe()

query = """
    SELECT * FROM `etl-testing-478716.firebase_etl_prod.userinvites`
"""
userinvites_df = bq_client.query(query).to_dataframe()

from process_daily_activity import create_user_daily_activity_table
from datetime import datetime

end_date = datetime.now().date()  # Set end date for filtering


daily_activity_df = create_user_daily_activity_table(posthog_events_df, events_df, userinvites_df, users_df, end_date)

print(daily_activity_df.head())