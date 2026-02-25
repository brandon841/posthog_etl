"""
Process People Data
====================

Extracts session data and aggregates along the user. 
Leaving a finalized table with one row per user, including behavioral flags (viewed event, joined event, created event, invited someone, enabled contacts, scrolled, visited discover, started quiz, completed quiz) and user metadata (city, country, user_id, fullName, phoneNumber, username, email, contactAccessGranted, businessUser).

Features:
- Query from sessions table in BigQuery to get session metadata (duration, timestamps, etc.)
- Query from users table in Firebase to get user metadata (phoneNumber, user_id, etc.)
- Query from events table in BigQuery to get event-level data for each person
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from google.cloud import bigquery
from etl_functions import ensure_required_columns, load_dataframe_to_bigquery

def define_schema_people():
    """Define schema for people_aggregated table
    
    User-level aggregated metrics from session data
    """
    return [
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("total_sessions", "INTEGER"),
        bigquery.SchemaField("avg_session_duration", "FLOAT"),
        bigquery.SchemaField("median_session_duration", "FLOAT"),
        bigquery.SchemaField("total_session_duration", "FLOAT"),
        bigquery.SchemaField("min_session_duration", "FLOAT"),
        bigquery.SchemaField("max_session_duration", "FLOAT"),
        bigquery.SchemaField("std_session_duration", "FLOAT"),
        bigquery.SchemaField("created_event_sum", "INTEGER"),
        bigquery.SchemaField("viewed_event_sum", "INTEGER"),
        bigquery.SchemaField("joined_event_sum", "INTEGER"),
        bigquery.SchemaField("invited_someone_sum", "INTEGER"),
        bigquery.SchemaField("enabled_contacts_sum", "INTEGER"),
        bigquery.SchemaField("scrolled_sum", "INTEGER"),
        bigquery.SchemaField("visited_discover_sum", "INTEGER"),
        bigquery.SchemaField("started_quiz_sum", "INTEGER"),
        bigquery.SchemaField("completed_quiz_sum", "INTEGER"),
        bigquery.SchemaField("total_scrolls", "INTEGER"),
        bigquery.SchemaField("avg_scrolls_per_session", "FLOAT"),
        bigquery.SchemaField("max_scrolls_per_session", "INTEGER"),
        bigquery.SchemaField("total_autocaptures", "INTEGER"),
        bigquery.SchemaField("avg_autocaptures_per_session", "FLOAT"),
        bigquery.SchemaField("max_autocaptures_per_session", "INTEGER"),
        bigquery.SchemaField("total_screens", "INTEGER"),
        bigquery.SchemaField("avg_screens_per_session", "FLOAT"),
        bigquery.SchemaField("max_screens_per_session", "INTEGER"),
        bigquery.SchemaField("first_session_date", "TIMESTAMP"),
        bigquery.SchemaField("last_session_date", "TIMESTAMP"),
        bigquery.SchemaField("fullName", "STRING"),
        bigquery.SchemaField("phoneNumber", "STRING"),
        bigquery.SchemaField("username", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("contactAccessGranted", "BOOLEAN"),
        bigquery.SchemaField("businessUser", "BOOLEAN"),
        bigquery.SchemaField("createdAt", "TIMESTAMP"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("days_since_first_session", "FLOAT"),
        bigquery.SchemaField("sessions_per_day", "FLOAT"),
        bigquery.SchemaField("engagement_score", "FLOAT"),
        bigquery.SchemaField("etl_loaded_at", "TIMESTAMP"),
    ]



def create_people_aggregated_df(session_aggregated_df): 


    # Aggregate session data to user level
    user_aggregated_df = session_aggregated_df.groupby('user_id').agg({
        # Session counts
        'session_id': 'count',
        
        # Duration statistics
        'session_duration': ['mean', 'median', 'sum', 'min', 'max', 'std'],
        
        # Event engagement metrics
        'created_event': 'sum',
        'viewed_event': 'sum',
        'joined_event': 'sum',
        'invited_someone': 'sum',
        'enabled_contacts': 'sum',
        'scrolled': 'sum',
        'visited_discover': 'sum',
        'started_quiz': 'sum',
        'completed_quiz': 'sum',
        
        # Activity counts
        'scroll_event_count': ['sum', 'mean', 'max'],
        'autocapture_count': ['sum', 'mean', 'max'],
        'screen_count': ['sum', 'mean', 'max'],
        
        # Temporal information
        'start_timestamp': ['min', 'max'],  # First and last session
        
        # User profile info (take first non-null value)
        'fullName': 'first',
        'phoneNumber': 'first',
        'username': 'first',
        'email': 'first',
        'contactAccessGranted': 'first',
        'businessUser': 'first',
        'createdAt': 'first',
        'city': 'first',
        'country': 'first'
    }).reset_index()

    # Flatten multi-level column names
    user_aggregated_df.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                                for col in user_aggregated_df.columns.values]

    # Rename columns for clarity
    user_aggregated_df.rename(columns={
        'session_id_count': 'total_sessions',
        'session_duration_mean': 'avg_session_duration',
        'session_duration_median': 'median_session_duration',
        'session_duration_sum': 'total_session_duration',
        'session_duration_min': 'min_session_duration',
        'session_duration_max': 'max_session_duration',
        'session_duration_std': 'std_session_duration',
        'start_timestamp_min': 'first_session_date',
        'start_timestamp_max': 'last_session_date',
        'scroll_event_count_sum': 'total_scrolls',
        'scroll_event_count_mean': 'avg_scrolls_per_session',
        'scroll_event_count_max': 'max_scrolls_per_session',
        'autocapture_count_sum': 'total_autocaptures',
        'autocapture_count_mean': 'avg_autocaptures_per_session',
        'autocapture_count_max': 'max_autocaptures_per_session',
        'screen_count_sum': 'total_screens',
        'screen_count_mean': 'avg_screens_per_session',
        'screen_count_max': 'max_screens_per_session',
        'fullName_first': 'fullName',
        'phoneNumber_first': 'phoneNumber',
        'username_first': 'username',
        'email_first': 'email',
        'contactAccessGranted_first': 'contactAccessGranted',
        'businessUser_first': 'businessUser',
        'createdAt_first': 'createdAt',
        'city_first': 'city',
        'country_first': 'country'
    }, inplace=True)

    # Calculate additional derived metrics
    user_aggregated_df['days_since_first_session'] = (
        user_aggregated_df['last_session_date'] - user_aggregated_df['first_session_date']
    ).dt.total_seconds() / 86400

    user_aggregated_df['sessions_per_day'] = (
        user_aggregated_df['total_sessions'] / 
        (user_aggregated_df['days_since_first_session'] + 1)  # Add 1 to avoid division by zero
    )


    # Engagement score (percentage of sessions with any activity)
    user_aggregated_df['engagement_score'] = (
        (user_aggregated_df['created_event_sum'] + 
        user_aggregated_df['viewed_event_sum'] + 
        user_aggregated_df['joined_event_sum'] + 
        user_aggregated_df['scrolled_sum']) / 
        user_aggregated_df['total_sessions']
    )

    # Add ETL timestamp
    user_aggregated_df['etl_loaded_at'] = pd.Timestamp.now(tz=timezone.utc)

    return user_aggregated_df


def process_people_data(aggregated_session_df,
                         bq_client=None, project_id=None, dataset_id=None):
    """
    Main processing function for people data
    
    Args:
        aggregated_session_df: DataFrame with aggregated session data
        bq_client: Optional BigQuery client for loading results
        project_id: Optional GCP project ID for loading results
        dataset_id: Optional BigQuery dataset ID for loading results
    
    Returns:
        Number of user records processed
    """
    print("\n" + "="*50)
    print("Processing People Data")
    print("="*50)

    # Create people aggregated DataFrame
    people_aggregated = create_people_aggregated_df(aggregated_session_df)

    print(f"\nProcessed {len(people_aggregated)} users")

    # Ensure all schema columns exist with proper defaults
    people_aggregated = ensure_required_columns(people_aggregated, define_schema_people)
    
    # Load to BigQuery if client and project info provided
    if bq_client and project_id and dataset_id:
        print("\nLoading people data to BigQuery...")
        load_dataframe_to_bigquery(
            df=people_aggregated,
            bq_client=bq_client,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id='people_aggregated',
            write_disposition='WRITE_TRUNCATE',  # Replace table each time
            schema=define_schema_people()  # Use defined schema
        )
    
    return len(people_aggregated)