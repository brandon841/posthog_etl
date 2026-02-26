


import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from google.cloud import bigquery
from functools import reduce
from etl_functions import ensure_required_columns, load_dataframe_to_bigquery

def define_schema_churn_state():
    """Define schema for user_churn_state table
    
    Churn state metrics for each user, tracking app and business activity
    """
    return [
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("app_churn_state", "STRING"),
        bigquery.SchemaField("app_churn_date", "DATE"),
        bigquery.SchemaField("app_times_churned", "INTEGER"),
        bigquery.SchemaField("days_since_last_app_activity", "INTEGER"),
        bigquery.SchemaField("first_app_active_date", "DATE"),
        bigquery.SchemaField("last_app_active_date", "DATE"),
        bigquery.SchemaField("biz_churn_state", "STRING"),
        bigquery.SchemaField("biz_churn_date", "DATE"),
        bigquery.SchemaField("biz_times_churned", "INTEGER"),
        bigquery.SchemaField("days_since_last_biz_activity", "INTEGER"),
        bigquery.SchemaField("first_biz_active_date", "DATE"),
        bigquery.SchemaField("last_biz_active_date", "DATE"),
        bigquery.SchemaField("total_events_created", "INTEGER"),
        bigquery.SchemaField("total_events_attended", "INTEGER"),
        bigquery.SchemaField("total_app_interactions", "INTEGER")
    ]

def create_user_churn_state_table(master_df, inactivity_threshold_days=14, end_date=None):
    """
    Create a user churn state table from the daily activity table.
    Calculates separate churn states for app activity and business activity.
    
    Parameters:
    - master_df: User daily activity DataFrame with user_id, date, and activity columns
    - inactivity_threshold_days: Number of days of inactivity before considering churned
    - end_date: The reference date for calculating current state (usually today)
    
    Returns:
    - DataFrame with current churn state per user
    """
    if end_date is None:
        end_date = pd.to_datetime('today').date()
    else:
        end_date = pd.to_datetime(end_date).date()
    
    # Define activity columns
    app_activity_cols = ['event_count']
    biz_activity_cols = ['events_created_count', 'accepted', 'invited', 'rejected']
    
    # Create flags for different types of activity
    master_df['has_app_activity'] = master_df[app_activity_cols].sum(axis=1) > 0
    master_df['has_biz_activity'] = master_df[biz_activity_cols].sum(axis=1) > 0
    
    # Group by user to calculate state metrics
    user_states = []
    
    # Define activity types to loop through
    activity_types = {
        'app': 'has_app_activity',
        'biz': 'has_biz_activity'
    }
    
    for user_id, user_data in master_df.groupby('user_id'):
        # Skip users with no activity at all
        if not user_data['has_app_activity'].any() and not user_data['has_biz_activity'].any():
            continue
        
        user_state = {'user_id': user_id}
        
        # Loop through each activity type
        for activity_name, activity_flag in activity_types.items():
            active_days = user_data[user_data[activity_flag]].copy()
            
            if len(active_days) > 0:
                first_active_date = active_days['date'].min()
                last_active_date = active_days['date'].max()
                days_since_last_activity = (end_date - last_active_date).days
                
                # Determine churn state
                if days_since_last_activity <= inactivity_threshold_days:
                    churn_state = 'active'
                    churn_date = None
                else:
                    churn_state = 'churned'
                    churn_date = last_active_date + pd.Timedelta(days=inactivity_threshold_days)
                
                # Count churn cycles
                active_sorted = active_days.sort_values('date')
                active_sorted['days_since_prev'] = active_sorted['date'].diff().apply(lambda x: x.days if pd.notna(x) else None)
                times_churned = (active_sorted['days_since_prev'] > inactivity_threshold_days).sum()
                
                # Check for reactivation
                if times_churned > 0 and churn_state == 'active':
                    churn_state = 'reactivated'
                
                # Store metrics for this activity type
                user_state[f'{activity_name}_churn_state'] = churn_state
                user_state[f'{activity_name}_churn_date'] = churn_date
                user_state[f'{activity_name}_times_churned'] = times_churned
                user_state[f'days_since_last_{activity_name}_activity'] = days_since_last_activity
                user_state[f'first_{activity_name}_active_date'] = first_active_date
                user_state[f'last_{activity_name}_active_date'] = last_active_date
            else:
                # No activity for this type
                user_state[f'{activity_name}_churn_state'] = 'never_active'
                user_state[f'{activity_name}_churn_date'] = None
                user_state[f'{activity_name}_times_churned'] = 0
                user_state[f'days_since_last_{activity_name}_activity'] = None
                user_state[f'first_{activity_name}_active_date'] = None
                user_state[f'last_{activity_name}_active_date'] = None
        
        # Determine user segment based on primary activity
        total_events_created = user_data['events_created_count'].sum()
        total_events_attended = user_data['accepted'].sum()
        total_app_interactions = user_data['event_count'].sum()

        #adding totals to the user state for later use in segmentation and analysis
        user_state['total_events_created'] = total_events_created
        user_state['total_events_attended'] = total_events_attended
        user_state['total_app_interactions'] = total_app_interactions
        
        user_states.append(user_state)
    
    return pd.DataFrame(user_states)

def process_churn_table(daily_activity_df, bq_client=None, project_id=None, dataset_id=None):
    """
    Main processing function for churn table
    
    Args:
        daily_activity_df: DataFrame with user daily activity data
        bq_client: BigQuery client for loading data
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
    """
    churn_df = create_user_churn_state_table(daily_activity_df)
    
    print(f"\nProcessed {len(churn_df)} churn state records")

    # Ensure all schema columns exist with proper defaults
    churn_df = ensure_required_columns(
        churn_df, define_schema_churn_state
    )
    
    # Load to BigQuery
    if bq_client and project_id and dataset_id:
        print("\nLoading user churn state data to BigQuery...")
        load_dataframe_to_bigquery(
            df=churn_df,
            bq_client=bq_client,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id='user_churn_state',
            write_disposition='WRITE_TRUNCATE',  # Replace table each time
            schema=define_schema_churn_state()  # Use defined schema
        )
    
    return churn_df