"""
Template for a modular ETL process step.
Use this pattern to build: process_users, process_events, etc.
"""

def run_example_process():
    print("[process_template] Running example ETL process step.")
    # Stub logic: do extract-transform-load here
    data = []  # extract step
    data = [record for record in data]  # transform step
    print("[process_template] Example ETL process completed.")