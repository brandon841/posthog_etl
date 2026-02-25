"""
Process Sessions Data
====================

Extracts PostHog events and sessions data, transforms it with behavioral metrics,
and loads session-level aggregations.

Features:
- Parallel queries to BigQuery for efficiency
- Extracts and parses event properties from JSON
- Aggregates events to session level with behavioral flags
- Merges with user and Firebase event data
- Filters out test/internal users
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from google.cloud import bigquery
from etl_functions import ensure_required_columns, load_dataframe_to_bigquery, get_excluded_users


def define_schema_sessions():
    """Define schema for sessions_aggregated table
    
    Core session columns with behavioral metrics and user metadata
    """
    return [
        bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("distinct_id", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("created_event", "BOOLEAN"),
        bigquery.SchemaField("viewed_event", "BOOLEAN"),
        bigquery.SchemaField("joined_event", "BOOLEAN"),
        bigquery.SchemaField("invited_someone", "BOOLEAN"),
        bigquery.SchemaField("enabled_contacts", "BOOLEAN"),
        bigquery.SchemaField("scrolled", "BOOLEAN"),
        bigquery.SchemaField("visited_discover", "BOOLEAN"),
        bigquery.SchemaField("scroll_event_count", "INTEGER"),
        bigquery.SchemaField("started_quiz", "BOOLEAN"),
        bigquery.SchemaField("completed_quiz", "BOOLEAN"),
        bigquery.SchemaField("start_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("end_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("autocapture_count", "INTEGER"),
        bigquery.SchemaField("screen_count", "INTEGER"),
        bigquery.SchemaField("session_duration", "FLOAT"),
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("fullName", "STRING"),
        bigquery.SchemaField("phoneNumber", "STRING"),
        bigquery.SchemaField("username", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("contactAccessGranted", "BOOLEAN"),
        bigquery.SchemaField("businessUser", "BOOLEAN"),
        bigquery.SchemaField("createdAt", "TIMESTAMP"),
        bigquery.SchemaField("etl_loaded_at", "TIMESTAMP"),
    ]


def extract_properties(row):
    """Extract properties from JSON string into separate columns"""
    prop = json.loads(row['properties'])
    return pd.Series({
        'event_name': row['event'],
        'session_id': prop.get('$session_id'),
        'lib': prop.get('$lib'),
        'screen_name': prop.get('$screen_name'),
        'distinct_id': row['distinct_id'],
        'city': prop.get('$geoip_city_name'),
        'country': prop.get('$geoip_country_name'),
        'touch_x': prop.get('$touch_x'),
        'touch_y': prop.get('$touch_y')
    })


def create_events_extracted_df(events_df, exclude_ids):
    """Create events_extracted dataframe with parsed properties
    
    Args:
        events_df: Raw events dataframe from BigQuery
        exclude_ids: List of user IDs to exclude
    
    Returns:
        DataFrame with extracted properties and filtered users
    """
    # Create new DataFrame with extracted columns
    events_extracted = events_df.apply(extract_properties, axis=1)
    
    # Filter to only "posthog-react-native" sessions and not excluded users
    events_extracted = events_extracted[
        (events_extracted['lib'] == 'posthog-react-native') &
        (~events_extracted['distinct_id'].isin(exclude_ids)) &
        (events_extracted['session_id'].notnull())
    ]
    
    return events_extracted


def create_session_aggregated_df(events_extracted, sessions_df, users_df, firebase_events_df, exclude_ids):
    """Create session-level aggregated dataframe with behavioral metrics
    
    This function aggregates event-level data to create one row per session with metrics like:
    - Did they view an event during a session?
    - Did they join the event during the session?
    - Did they create an event during the session?
    - Did they invite someone during the session?
    - Did they enable contacts during the session?
    - Did they scroll during a session?
    - Did they go to the Discover page during a session?
    - How long did they scroll during a session?
    - Did they take the quiz during a session?
    
    Args:
        events_extracted: Event-level dataframe with extracted properties
        sessions_df: Session metadata from BigQuery (includes session_duration, timestamps, etc.)
        users_df: User metadata from Firebase (phoneNumber, user_id, etc.)
        firebase_events_df: Firebase events with createdAt timestamps
        exclude_ids: List of user IDs to exclude
    
    Returns:
        DataFrame with one row per session including behavioral and user metadata
    """
    # Group by session and aggregate event-level data
    session_agg = events_extracted.groupby('session_id').agg({
        'distinct_id': 'first',  # User ID (should be same for all events in session)
        'event_name': lambda x: list(x),  # Keep all events for analysis
        'screen_name': lambda x: list(x),  # Keep all screen names
        'touch_x': lambda x: x.notnull().sum(),  # Count touch events (scroll indicator)
        'touch_y': lambda x: x.notnull().sum(),  # Count touch events (scroll indicator)
        'city': 'first',  # City from first event in session
        'country': 'first',  # Country from first event in session
    }).reset_index()
    
    # Calculate behavioral flags
    session_agg['viewed_event'] = session_agg['event_name'].apply(
        lambda events: any(str(e) == 'view_event' for e in events)
    )
    
    session_agg['joined_event'] = session_agg['event_name'].apply(
        lambda events: any(str(e) == 'join_event' for e in events)
    )
    
    session_agg['invited_someone'] = session_agg['event_name'].apply(
        lambda events: any(str(e) in ['invite_friends', 'send_invite_to_event', 'invite_friends_for_report'] for e in events)
    )
    
    session_agg['enabled_contacts'] = session_agg['event_name'].apply(
        lambda events: any('contact' in str(e).lower() and 'enable' in str(e).lower() for e in events)
    )

    # Scrolling logic: If there are any touch events (touch_x or touch_y), we can infer that the user scrolled during the session
    #TO DO can we caclculate scroll duration or depth of scroll?
    session_agg['scrolled'] = (session_agg['touch_x'] > 0) | (session_agg['touch_y'] > 0)
    
    session_agg['visited_discover'] = session_agg['screen_name'].apply(
        lambda screens: 'Discover' in screens
    )
    
    # Count scroll events (touch events as proxy for scrolling)
    session_agg['scroll_event_count'] = session_agg['touch_x'] + session_agg['touch_y']
    
    session_agg['started_quiz'] = session_agg['event_name'].apply(
        lambda events: any(str(e) == 'start_quiz' for e in events)
    )
    
    session_agg['completed_quiz'] = session_agg['event_name'].apply(
        lambda events: any(str(e) == 'finish_quiz' for e in events)
    )
    
    # Drop the intermediate list columns
    session_agg = session_agg.drop(['event_name', 'screen_name', 'touch_x', 'touch_y'], axis=1)
    
    # Merge with sessions_df to get session metadata (duration, timestamps, etc.)
    sessions_filtered = sessions_df[~sessions_df['distinct_id'].isin(exclude_ids)]
    
    session_final = session_agg.merge(
        sessions_filtered,
        on=['session_id', 'distinct_id'],
        how='left'
    )

    users_df = users_df.sort_values(['fullName', 'phoneNumber',
                                      'username', 'email']).drop_duplicates(subset=['fullName',
                                                                                     'phoneNumber'], keep='first')
    
    # Merge with users_df to get user metadata (first on phoneNumber)
    merged_phone = session_final.merge(
        users_df,
        left_on='distinct_id',
        right_on='phoneNumber',
        how='left',
        suffixes=('', '_user')
    )

    # Find rows where user info was not found (i.e., user_id is NaN after phoneNumber merge)
    unmatched = merged_phone[merged_phone['user_id'].isna()].copy()

    if not unmatched.empty:
        # Try to merge those on user_id
        unmatched = unmatched.drop(users_df.columns, axis=1, errors='ignore')  # Remove user columns to avoid conflicts
        merged_userid = unmatched.merge(
            users_df,
            left_on='distinct_id',
            right_on='user_id',
            how='left',
            suffixes=('', '_user')
        )
        # # Keep only rows where user_id is now found
        # matched_userid = merged_userid[~merged_userid['user_id'].isna()]
        # Keep rows from the first merge where user_id was found
        matched_phone = merged_phone[~merged_phone['user_id'].isna()]
        # Combine both sets
        session_final = pd.concat([matched_phone, merged_userid], ignore_index=True)
    else:
        session_final = merged_phone
    
    # Determine if user created an event during the session
    # Check if any events in firebase_events_df were created within the session time range
    def check_event_creation(row):
        if pd.isna(row.get('user_id')) or pd.isna(row.get('start_timestamp')) or pd.isna(row.get('end_timestamp')):
            return False
        
        user_events = firebase_events_df[firebase_events_df['user_id'] == row['user_id']]
        if user_events.empty:
            return False
        
        # Check if any event was created within this session's time range
        created_during_session = user_events[
            (user_events['createdAt'] >= row['start_timestamp']) &
            (user_events['createdAt'] <= row['end_timestamp'])
        ]
        
        return len(created_during_session) > 0
    
    session_final['created_event'] = session_final.apply(check_event_creation, axis=1)

    session_final = session_final.dropna(subset=['user_id'])

    columns_to_show = ['session_id', 'distinct_id', 'city', 'country', 'created_event', 'viewed_event', 'joined_event',
       'invited_someone', 'enabled_contacts', 'scrolled', 'visited_discover',
       'scroll_event_count', 'started_quiz', 'completed_quiz', 'start_timestamp', 'end_timestamp',
       'autocapture_count', 'screen_count', 'session_duration',
        'user_id', 'fullName', 'phoneNumber',
       'username', 'email', 'contactAccessGranted',
       'businessUser', 'createdAt']

    session_final = session_final[[col for col in columns_to_show if col in session_final.columns]]

    #Adding etl_loaded_at timestamp
    session_final['etl_loaded_at'] = datetime.now(timezone.utc)
    
    return session_final


def process_sessions_data(posthog_events_df, sessions_df, users_df, firebase_events_df,
                         bq_client=None, project_id=None, dataset_id=None):
    """
    Main processing function for sessions data
    
    Args:
        posthog_events_df: DataFrame with PostHog events
        sessions_df: DataFrame with PostHog sessions
        users_df: DataFrame with Firebase users
        firebase_events_df: DataFrame with Firebase events
        bq_client: Optional BigQuery client for loading results
        project_id: Optional GCP project ID for loading results
        dataset_id: Optional BigQuery dataset ID for loading results
    
    Returns:
        DataFrame with session records processed
    """
    print("\n" + "="*50)
    print("Processing Sessions Data")
    print("="*50)
    
    # Define excluded user IDs (test/internal users)
    exclude_ids = get_excluded_users()
    
    # Create events_extracted dataframe
    print("Extracting event properties...")
    events_extracted = create_events_extracted_df(posthog_events_df, exclude_ids)
    print(f"Filtered to {len(events_extracted)} posthog-react-native events from non-excluded users")
    
    # Create session-level aggregated dataframe
    print("Creating session-level aggregated dataframe...")
    session_aggregated = create_session_aggregated_df(
        events_extracted, sessions_df, users_df, firebase_events_df, exclude_ids
    )
    
    print(f"Session Aggregated DataFrame shape: {session_aggregated.shape}")
    print(f"Total sessions processed: {len(session_aggregated)}")

    # Ensure all schema columns exist with proper defaults
    session_aggregated = ensure_required_columns(session_aggregated, define_schema_sessions)
    
    # Load to BigQuery if client and project info provided
    if bq_client and project_id and dataset_id:
        print("\nLoading sessions data to BigQuery...")
        load_dataframe_to_bigquery(
            df=session_aggregated,
            bq_client=bq_client,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id='sessions_aggregated',
            write_disposition='WRITE_TRUNCATE',  # Replace table each time
            schema=define_schema_sessions()  # Use defined schema
        )
    
    return session_aggregated