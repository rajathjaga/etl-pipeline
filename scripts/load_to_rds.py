import pandas as pd
import logging

def load_to_rds(engine, table_name, file_path):
    """Load transformed data into AWS RDS."""
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    logging.info(f"Data from {file_path} loaded into RDS table {table_name}.")