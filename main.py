import etl_functions
import process_template  # Add your new ETL step modules here

def main():
    print("Starting ETL Pipeline Framework...")

    # Example of running a process step (stub)
    data_extracted = etl_functions.extract()
    data_transformed = etl_functions.transform(data_extracted)
    etl_functions.load(data_transformed)

    # Run any number of process modules (expand as needed)
    process_template.run_example_process()

    print("Pipeline finished.")

if __name__ == "__main__":
    main()