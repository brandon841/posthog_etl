


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
        bigquery.SchemaField("total_app_interactions", "INTEGER"),
        bigquery.SchemaField("etl_loaded_at", "TIMESTAMP")
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
    
    # Calculate activity totals first (for later use)
    activity_totals = master_df.groupby('user_id').agg({
        'events_created_count': 'sum',
        'accepted': 'sum',
        'event_count': 'sum'
    }).rename(columns={
        'events_created_count': 'total_events_created',
        'accepted': 'total_events_attended',
        'event_count': 'total_app_interactions'
    })
    
    # Process each activity type separately using vectorized operations
    user_states_list = []
    
    # Define activity types to process
    activity_types = {
        'app': 'has_app_activity',
        'biz': 'has_biz_activity'
    }
    
    for activity_name, activity_flag in activity_types.items():
        # Filter to only active days for this activity type
        active_only = master_df[master_df[activity_flag]].copy()
        
        if len(active_only) == 0:
            continue
            
        # Group by user and aggregate
        user_agg = active_only.groupby('user_id')['date'].agg(['min', 'max', 'count']).reset_index()
        user_agg.columns = ['user_id', 'first_active', 'last_active', 'active_day_count']
        
        # Calculate days since last activity
        user_agg['days_since_last'] = (end_date - user_agg['last_active']).apply(lambda x: x.days)
        
        # Determine churn state
        user_agg['churn_state'] = np.where(
            user_agg['days_since_last'] <= inactivity_threshold_days,
            'active',
            'churned'
        )
        
        # Calculate churn date
        user_agg['churn_date'] = np.where(
            user_agg['churn_state'] == 'churned',
            user_agg['last_active'] + pd.Timedelta(days=inactivity_threshold_days),
            None
        )
        
        # Count churn cycles - calculate gaps between consecutive active days per user
        active_sorted = active_only.sort_values(['user_id', 'date']).copy()
        active_sorted['days_since_prev'] = active_sorted.groupby('user_id')['date'].diff().apply(
            lambda x: x.days if pd.notna(x) else 0
        )
        churn_counts = active_sorted[active_sorted['days_since_prev'] > inactivity_threshold_days].groupby('user_id').size()
        churn_counts = churn_counts.reindex(user_agg['user_id'], fill_value=0)
        user_agg['times_churned'] = churn_counts.values
        
        # Check for reactivation
        user_agg.loc[(user_agg['times_churned'] > 0) & (user_agg['churn_state'] == 'active'), 'churn_state'] = 'reactivated'
        
        # Rename columns with activity prefix
        user_agg = user_agg.rename(columns={
            'churn_state': f'{activity_name}_churn_state',
            'churn_date': f'{activity_name}_churn_date',
            'times_churned': f'{activity_name}_times_churned',
            'days_since_last': f'days_since_last_{activity_name}_activity',
            'first_active': f'first_{activity_name}_active_date',
            'last_active': f'last_{activity_name}_active_date'
        }).drop('active_day_count', axis=1)
        
        user_states_list.append(user_agg)
    
    # Merge all activity types
    if len(user_states_list) == 0:
        return pd.DataFrame()
    
    user_states = user_states_list[0]
    for df in user_states_list[1:]:
        user_states = user_states.merge(df, on='user_id', how='outer')
    
    # Add activity totals
    user_states = user_states.merge(activity_totals, on='user_id', how='left')
    
    # Fill missing activity type states with 'never_active'
    for activity_name in activity_types.keys():
        state_col = f'{activity_name}_churn_state'
        if state_col not in user_states.columns:
            user_states[state_col] = 'never_active'
            user_states[f'{activity_name}_churn_date'] = None
            user_states[f'{activity_name}_times_churned'] = 0
            user_states[f'days_since_last_{activity_name}_activity'] = None
            user_states[f'first_{activity_name}_active_date'] = None
            user_states[f'last_{activity_name}_active_date'] = None
        else:
            user_states[state_col] = user_states[state_col].fillna('never_active')
            user_states[f'{activity_name}_times_churned'] = user_states[f'{activity_name}_times_churned'].fillna(0)
    
    return user_states

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

    #adding etl_loaded_at timestamp for record-keeping
    churn_df['etl_loaded_at'] = pd.Timestamp.now(tz=timezone.utc)

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