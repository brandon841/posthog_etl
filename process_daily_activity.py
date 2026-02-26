


import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from google.cloud import bigquery
from functools import reduce
from etl_functions import ensure_required_columns, load_dataframe_to_bigquery, get_excluded_users

def define_schema_user_daily_activity():
    """Define schema for user_daily_activity table
    
    Daily activity metrics for each user, including event counts and user info
    """
    return [
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("event_count", "INTEGER"),
        bigquery.SchemaField("events_created_count", "INTEGER"),
        bigquery.SchemaField("accepted", "INTEGER"),
        bigquery.SchemaField("invited", "INTEGER"),
        bigquery.SchemaField("rejected", "INTEGER"),
        bigquery.SchemaField("createdAt", "TIMESTAMP"),
        bigquery.SchemaField("phoneNumber", "STRING"),
        bigquery.SchemaField("fullName", "STRING"),
        bigquery.SchemaField("username", "STRING"),
        bigquery.SchemaField("email", "STRING")
    ]

#function to create user specific date grid
def create_user_specific_date_grid(master_df, end_date):
    """
    Create a user-date grid where each user has rows from their creation date 
    (or first activity if createdAt is missing) to the end date.
    
    Parameters:
    - master_df: DataFrame with user_id, date, and createdAt columns
    - end_date: The end date for all users (string or datetime)
    
    Returns:
    - DataFrame with user_id and date columns
    """
    end_date = pd.to_datetime(end_date).date()
    
    # Get each user's start date (createdAt or first activity)
    user_start_dates = master_df.groupby('user_id').agg({
        'createdAt': 'first',  # Get createdAt (should be same for all rows of a user)
        'date': 'min'  # Get first activity date as fallback
    }).reset_index()
    
    # Normalize createdAt to timezone-naive and convert to date
    user_start_dates['createdAt_date'] = pd.to_datetime(
        user_start_dates['createdAt']
    ).dt.tz_localize(None).dt.date
    
    # Ensure date column is also date type
    user_start_dates['date'] = pd.to_datetime(user_start_dates['date']).dt.date
    
    # Use createdAt if available, otherwise use first activity date
    user_start_dates['start_date'] = user_start_dates['createdAt_date'].fillna(
        user_start_dates['date']
    )
    
    # Create date range for each user
    all_user_dates = []
    for _, row in user_start_dates.iterrows():
        user_id = row['user_id']
        start = row['start_date']
        
        # Generate date range for this user
        dates = pd.date_range(start=start, end=end_date, freq='D').date
        user_dates = pd.DataFrame({
            'user_id': user_id,
            'date': dates
        })
        all_user_dates.append(user_dates)
    
    # Concatenate all user date ranges
    return pd.concat(all_user_dates, ignore_index=True)

#Function to create user daily activity table
def create_user_daily_activity_table(posthog_events_df, events_df, userinvites_df, users_df, end_date):
    """
    Create a daily activity table with one row per user per day
    
    Aggregates:
    - PostHog events (event_count)
    - Firebase events created (events_created_count)
    - User invites by status (accepted, invited, rejected)
    """
    # Get excluded users list
    excluded_users = get_excluded_users()
    
    # ============================================================================
    # Prepare deduplicated user lookup - keep rows with most information
    # ============================================================================
    # Sort users_df to prioritize rows with more complete information
    # Non-null email and username should come first
    users_sorted = users_df.copy()
    users_sorted['email_filled'] = users_sorted['email'].notna()
    users_sorted['username_filled'] = users_sorted['username'].notna()
    users_sorted['createdAt_filled'] = users_sorted['createdAt'].notna()
    
    users_sorted = users_sorted.sort_values(
        by=['email_filled', 'username_filled', 'createdAt_filled'],
        ascending=[False, False, False]
    )
    
    # Drop helper columns
    users_sorted = users_sorted.drop(['email_filled', 'username_filled', 'createdAt_filled'], axis=1)
    
    # Deduplicate: prioritize phoneNumber deduplication, then user_id
    # For users with phone numbers: keep most complete record per phoneNumber
    users_with_phone = users_sorted[users_sorted['phoneNumber'].notna()].drop_duplicates('phoneNumber', keep='first')
    
    # For users without phone numbers: keep one record per user_id
    users_without_phone = users_sorted[users_sorted['phoneNumber'].isna()].drop_duplicates('user_id', keep='first')
    
    # Combine both
    users_deduped = pd.concat([users_with_phone, users_without_phone], ignore_index=True)
    
    # Create lookup tables
    user_lookup = users_deduped[['user_id', 'phoneNumber', 'fullName', 'username', 'email', 'createdAt']]
    phone_mapping = users_deduped[['user_id', 'phoneNumber']].dropna(subset=['phoneNumber'])
    
    # ============================================================================
    # 1. Process PostHog events: Map distinct_id to user_id
    # ============================================================================
    grouped_posthog = posthog_events_df.groupby(
        ['distinct_id', pd.Grouper(key='timestamp', freq='D')]
    ).size().reset_index(name='event_count')
    grouped_posthog.rename(columns={'timestamp': 'date'}, inplace=True)
    
    # Try to map distinct_id to user_id via phoneNumber first
    mapped_by_phone = grouped_posthog.merge(
        phone_mapping,
        left_on='distinct_id',
        right_on='phoneNumber',
        how='left'
    )
    
    # Try to map remaining unmapped rows via user_id
    unmapped_mask = mapped_by_phone['user_id'].isna()
    if unmapped_mask.any():
        userid_mapping = users_sorted[['user_id']].drop_duplicates('user_id', keep='first')
        unmapped_df = mapped_by_phone.loc[unmapped_mask].drop('user_id', axis=1)
        remapped = unmapped_df.merge(
            userid_mapping,
            left_on='distinct_id',
            right_on='user_id',
            how='left'
        )
        mapped_by_phone.loc[unmapped_mask, 'user_id'] = remapped['user_id'].values
    
    # Keep only successfully mapped rows with valid user_ids
    posthog_activity = mapped_by_phone[mapped_by_phone['user_id'].notna()].copy()
    
    # Select final columns
    posthog_activity = posthog_activity[['user_id', 'date', 'event_count']]
    
    # ============================================================================
    # 2. Process Firebase events created (already has user_id)
    # ============================================================================
    grouped_events = events_df.groupby(
        ['user_id', pd.Grouper(key='createdAt', freq='D')]
    ).size().reset_index(name='events_created_count')
    grouped_events.rename(columns={'createdAt': 'date'}, inplace=True)
    
    # ============================================================================
    # 3. Process user invites by status (already has user_id)
    # ============================================================================
    grouped_invites = userinvites_df.groupby(
        ['user_id', pd.Grouper(key='createdAt', freq='D'), 'status']
    ).size().reset_index(name='count')
    grouped_invites.rename(columns={'createdAt': 'date'}, inplace=True)
    
    # Pivot to get status as columns
    invites_pivoted = pd.pivot_table(
        grouped_invites,
        index=['user_id', 'date'],
        columns='status',
        values='count',
        fill_value=0
    ).reset_index()
    
    # ============================================================================
    # 4. Merge all activity metrics (without user info yet)
    # ============================================================================
    activity_dfs = [posthog_activity, grouped_events, invites_pivoted]
    master_df = reduce(
        lambda left, right: pd.merge(left, right, on=['user_id', 'date'], how='outer'),
        activity_dfs
    )
    
    # Filter out excluded users (now that all data is merged)
    # First, need to get phoneNumber mapping for user_ids to filter properly
    user_phone_mapping = user_lookup[['user_id', 'phoneNumber']].dropna(subset=['phoneNumber'])
    excluded_user_ids = user_phone_mapping[
        user_phone_mapping['phoneNumber'].isin(excluded_users)
    ]['user_id'].unique()
    
    initial_count = len(master_df)
    master_df = master_df[~master_df['user_id'].isin(excluded_user_ids)]
    print(f"Filtered out {initial_count - len(master_df)} records from excluded users")
    
    # Convert date to date type
    master_df['date'] = pd.to_datetime(master_df['date']).dt.date
    
    # Fill NaN activity counts with 0
    activity_cols = ['event_count', 'events_created_count']
    # Add status columns if they exist
    for col in ['accepted', 'invited', 'rejected']:
        if col in master_df.columns:
            activity_cols.append(col)
    master_df[activity_cols] = master_df[activity_cols].fillna(0)
    
    # ============================================================================
    # 5. Consolidate duplicate user_ids to canonical versions BEFORE date grid
    # ============================================================================
    # This ensures all activity for duplicate user_ids is grouped under one canonical user_id
    
    # Create a mapping from all user_ids to their canonical user_id (based on phoneNumber)
    # For users in user_lookup, they map to themselves
    canonical_mapping = user_lookup[['user_id']].copy()
    canonical_mapping['canonical_user_id'] = canonical_mapping['user_id']
    
    # For users not in user_lookup but in original users_df with same phone, map to canonical
    phone_to_canonical = user_lookup[['phoneNumber', 'user_id']].dropna(subset=['phoneNumber']).rename(
        columns={'user_id': 'canonical_user_id'}
    )
    
    # Get non-canonical users (those not in user_lookup but share a phone with someone in it)
    non_canonical = users_df[~users_df['user_id'].isin(user_lookup['user_id'])][
        ['user_id', 'phoneNumber']
    ].dropna(subset=['phoneNumber'])
    non_canonical = non_canonical.merge(phone_to_canonical, on='phoneNumber', how='inner')
    
    # Combine mappings
    full_mapping = pd.concat([
        canonical_mapping[['user_id', 'canonical_user_id']],
        non_canonical[['user_id', 'canonical_user_id']]
    ], ignore_index=True).drop_duplicates('user_id')
    
    # Apply mapping to master_df: replace user_id with canonical_user_id
    master_df = master_df.merge(full_mapping, on='user_id', how='left')
    master_df['user_id'] = master_df['canonical_user_id'].fillna(master_df['user_id'])
    master_df.drop('canonical_user_id', axis=1, inplace=True)
    
    # Group by canonical user_id and date, summing activity metrics
    group_cols = ['user_id', 'date']
    agg_dict = {col: 'sum' for col in activity_cols}
    
    master_df = master_df.groupby(group_cols, as_index=False).agg(agg_dict)
    
    # ============================================================================
    # 6. Add createdAt from user_lookup (now with canonical user_ids only)
    # ============================================================================
    master_df = master_df.merge(
        user_lookup[['user_id', 'createdAt']],
        on='user_id',
        how='left'
    )
    
    # ============================================================================
    # 7. Create date grid (now each canonical user has one date range)
    # ============================================================================
    user_date_grid = create_user_specific_date_grid(master_df, end_date)
    
    # Merge grid with activity data
    master_df = user_date_grid.merge(master_df, on=['user_id', 'date'], how='left')
    
    # Fill NaN activity counts with 0 again after grid merge
    master_df[activity_cols] = master_df[activity_cols].fillna(0)
    
    # ============================================================================
    # 8. Add final user info
    # ============================================================================
    master_df = master_df.merge(
        user_lookup[['user_id', 'phoneNumber', 'fullName', 'username', 'email']],
        on='user_id',
        how='left'
    )
    
    return master_df

def process_daily_activity(posthog_events_df, events_df, userinvites_df, users_df, bq_client, project_id, dataset_id):
    """
    Main processing function for user daily activity data
    
    Args:
        posthog_events_df: DataFrame with PostHog events
        events_df: DataFrame with Firebase events
        userinvites_df: DataFrame with user invites
        users_df: DataFrame with user info
        bq_client: BigQuery client for loading results
        project_id: GCP project ID for loading results
        dataset_id: BigQuery dataset ID for loading results
    Returns:
        DataFrame with user daily activity data
    """
    print("\n" + "="*50)
    print("Processing User Daily Activity Data")
    print("="*50)

    # Create user daily activity table
    end_date = datetime.now().date()  # Use current date as end date for grid
    user_daily_activity_df = create_user_daily_activity_table(
        posthog_events_df, events_df, userinvites_df, users_df, end_date
    )

    print(f"\nProcessed {len(user_daily_activity_df)} user daily activity records")

    # Ensure all schema columns exist with proper defaults
    user_daily_activity_df = ensure_required_columns(
        user_daily_activity_df, define_schema_user_daily_activity
    )
    
    # Load to BigQuery
    if bq_client and project_id and dataset_id:
        print("\nLoading user daily activity data to BigQuery...")
        load_dataframe_to_bigquery(
            df=user_daily_activity_df,
            bq_client=bq_client,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id='user_daily_activity',
            write_disposition='WRITE_TRUNCATE',  # Replace table each time
            schema=define_schema_user_daily_activity()  # Use defined schema
        )

    return user_daily_activity_df