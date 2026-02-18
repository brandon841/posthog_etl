"""
Generic ETL operation stubs.  
Implement your real Extract, Transform, and Load logic here!
"""

def extract():
    print("[etl_functions] Extract step")
    # TODO: Replace with real extraction code
    # e.g. fetch from API, database, file, etc.
    data = []
    return data

def transform(data):
    print("[etl_functions] Transform step")
    # TODO: Your cleaning, mapping, enriching, etc.
    return data

def load(data):
    print("[etl_functions] Load step")
    # TODO: Send to database, file, another service, etc.
    print("[etl_functions] Data loaded (nothing actually loaded in this stub)")