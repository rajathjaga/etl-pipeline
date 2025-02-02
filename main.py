import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from scripts.download_and_unzip_data import download_and_unzip_data
from scripts.s3_operations import upload_to_s3
from scripts.file_transform import transform_csv, transform_json, transform_xml
from scripts.load_to_rds import load_to_rds
from datetime import datetime

load_dotenv()

log_filename = f'./logs/etl_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    filename=log_filename, 
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

if __name__ == "__main__":
    # Constants
    DATA_URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip"
    DOWNLOAD_PATH = "./data/source.zip"
    EXTRACT_TO = "./data/unzipped_folder"
    S3_BUCKET = "my-etl-project-bucket"
    RDS_DB = os.getenv("RDS_DB")

    # Step 1: Download and Unzip Data
    download_and_unzip_data(DATA_URL, DOWNLOAD_PATH, EXTRACT_TO)

    # Step 2: AWS Setup - Upload raw data to S3
    for file_name in os.listdir(EXTRACT_TO):
        file_path = os.path.join(EXTRACT_TO, file_name)
        upload_to_s3(S3_BUCKET, file_path, f"raw/{file_name}")

    # Step 3: Extract, Transform, and Load
    engine = create_engine(RDS_DB)
    
    for file_name in os.listdir(EXTRACT_TO):
        file_path = os.path.join(EXTRACT_TO, file_name)

        if file_name.endswith('.csv'):
            transformed_path = transform_csv(file_path)
        elif file_name.endswith('.json'):
            transformed_path = transform_json(file_path)
        elif file_name.endswith('.xml'):
            transformed_path = transform_xml(file_path)
        else:
            logging.warning(f"Unsupported file format: {file_name}")
            continue

    # Initialize an empty DataFrame to store all transformed data
    all_data = pd.DataFrame()

    # Iterate over CSV files in the transformed directory
    transformed_dir = './data/transformed'
    for file_name in os.listdir(transformed_dir):
        file_path = os.path.join(transformed_dir, file_name)

        if file_name.endswith('.csv'):
            # Read each CSV file into a DataFrame
            transformed_data = pd.read_csv(file_path)
            # Append the DataFrame to all_data
            all_data = pd.concat([all_data, transformed_data], ignore_index=True)
        else:
            logging.warning(f"Unsupported file format in transformed directory: {file_name}")

    # Save the concatenated DataFrame to a single CSV file
    all_data.to_csv('./data/concatenated_data.csv', index=False)

    # Upload transformed data to S3
    upload_to_s3(S3_BUCKET, './data/concatenated_data.csv', f"transformed/transformed_data.csv")

    # Load data into RDS
    table_name = 'user_metrics'
    load_to_rds(engine, table_name, './data/concatenated_data.csv')

    logging.info("ETL pipeline executed successfully.")

    # Upload the log file to S3
    upload_to_s3(S3_BUCKET, log_filename, "logs/etl_pipeline.log")
