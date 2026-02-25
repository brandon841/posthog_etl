"""
Generic ETL operation stubs.  
Implement your real Extract, Transform, and Load logic here!
"""

from concurrent.futures import ThreadPoolExecutor


import pandas as pd
from datetime import datetime, timezone
import re
from typing import List, Dict, Any, Optional

import firebase_admin
from firebase_admin import credentials, firestore

# BigQuery imports (for incremental ETL functions)
try:
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
except ImportError:
    print("⚠ Warning: BigQuery dependencies not installed. Incremental functions will not work.")
    print("Install with: pip install google-cloud-bigquery")



def query_bq(bq_client, query):
    """Execute a BigQuery query and return results as a DataFrame"""
    return bq_client.query(query).to_dataframe()


def query_bq_parallel(bq_client, queries):
    """Execute multiple BigQuery queries in parallel using ThreadPoolExecutor
    
    Args:
        bq_client: BigQuery client instance
        queries: List of tuples (query_name, query_string)
    
    Returns:
        Dictionary mapping query names to resulting DataFrames
    """
    def run_query(query_tuple):
        name, query = query_tuple
        return name, query_bq(bq_client, query)
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = executor.map(run_query, queries)
    
    return dict(results)

def get_excluded_users() -> List[str]:
    """Return a list of user IDs to exclude from processing (e.g., test accounts)"""
    # Define excluded user IDs (test/internal users)
    bok_ids = ['+18323900558', '+18323875995', '+18323787163', '+11111111111']
    maaz_ids = []
    taras_ids = ['+15126437937']
    zach_ids = ['+15126437937', '+15125577162']
    trask_ids = ['+12146865810']
    brandon_ids = ['+15126530534']
    exclude_ids = bok_ids + maaz_ids + taras_ids + zach_ids + trask_ids + brandon_ids
    
    return exclude_ids


# ============================================================================
#BIG QUERY INTEGRATION FUNCTIONS
# ============================================================================
#region BigQuery Integration Functions

def ensure_required_columns(df: pd.DataFrame, schema_function) -> pd.DataFrame:
    """
    Ensure all required columns exist in DataFrame based on BigQuery schema definition.
    
    Args:
        df: Input DataFrame
        schema_function: Function that returns BigQuery schema (e.g., define_schema_users)
    
    Returns:
        DataFrame with all required columns from schema
    """
    # Get schema from the schema function
    schema = schema_function()
    
    # Extract column names and types from schema
    for field in schema:
        col_name = field.name
        field_type = field.field_type
        
        if col_name not in df.columns:
            # Set appropriate defaults based on BigQuery field type
            if field_type == 'BOOLEAN':
                df[col_name] = False
            elif field_type in ['INTEGER', 'FLOAT', 'NUMERIC']:
                df[col_name] = None
            elif field_type in ['TIMESTAMP', 'DATETIME', 'DATE']:
                df[col_name] = None
            else:  # STRING and other types
                df[col_name] = None
    
    # Fill NaN values for boolean columns
    boolean_fields = [field.name for field in schema if field.field_type == 'BOOLEAN']
    for col in boolean_fields:
        if col in df.columns:
            df.loc[:, col] = df[col].fillna(False)
    
    # Return DataFrame with only schema columns in the right order
    schema_columns = [field.name for field in schema]
    available_columns = [col for col in schema_columns if col in df.columns]
    
    # Create a clean copy with reset index to avoid PyArrow conversion issues
    result_df = df[available_columns].copy()
    result_df.reset_index(drop=True, inplace=True)
    
    return result_df

def create_bigquery_dataset(bq_client: bigquery.Client, dataset_id: str, 
                           location: str = 'US') -> bigquery.Dataset:
    """
    Create BigQuery dataset if it doesn't exist
    """
    dataset_ref = f"{bq_client.project}.{dataset_id}"
    
    try:
        dataset = bq_client.get_dataset(dataset_ref)
        print(f"✓ Dataset {dataset_id} already exists")
        return dataset
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset = bq_client.create_dataset(dataset, exists_ok=True)
        print(f"✓ Created dataset {dataset_id}")
        return dataset
    
def load_dataframe_to_bigquery(
    df: pd.DataFrame,
    bq_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    write_disposition: str = 'WRITE_APPEND',  # or 'WRITE_TRUNCATE'
    schema: Optional[List[bigquery.SchemaField]] = None
) -> None:
    """
    Load a pandas DataFrame to BigQuery
    
    Args:
        df: DataFrame to load
        bq_client: BigQuery client
        project_id: GCP project ID
        dataset_id: Dataset name
        table_id: Table name
        write_disposition: 'WRITE_APPEND' (add rows) or 'WRITE_TRUNCATE' (replace)
        schema: Optional schema definition (auto-detected if None)
    """
    if df.empty:
        print(f"⚠ Skipping {table_id}: DataFrame is empty")
        return
    
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    print(f"\nLoading {len(df)} rows to {table_ref}...")
    
    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema,
        # Auto-detect schema if not provided
        autodetect=True if schema is None else False
    )
    
    try:
        # Load DataFrame to BigQuery
        job = bq_client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()  # Wait for completion
        
        print(f"✓ Successfully loaded {len(df)} rows to {table_id}")
        
        # Print table info
        table = bq_client.get_table(table_ref)
        print(f"  Total rows in table: {table.num_rows:,}")
        
    except Exception as e:
        print(f"✗ Error loading to {table_id}: {e}")
        raise
