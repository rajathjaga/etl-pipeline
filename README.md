# Enhanced ETL Workflow with Python, AWS S3, RDS, and Glue for Data Engineers

## Overview

This ETL pipeline downloads, extracts, transforms, and loads data from multiple file formats (CSV, JSON, XML). The transformed data is uploaded to an AWS S3 bucket, and the final processed data is stored in an RDS database.

##

- **Extract:** Downloads and unzips raw data files, Upload the raw files to s3 bucket.
- **Transform:** Converts height (inches to meters) and weight (lbs to kg).
- **Load:** Uploads transformed data to S3 and RDS.
- **Combine:** Merges transformed data from all sources into a single CSV file and uploads it to S3.

## Directory Structure

```
├── data
│   ├── source.zip          # Downloaded ZIP file
│   ├── unzipped_folder     # Extracted raw data
│   ├── combined_data.csv   # Final merged data
├── logs
│   ├── etl_pipeline.log    # Log file
├── scripts
│   ├── download_and_unzip_data.py  # Handles downloading and extracting
│   ├── file_transform.py           # Performs data transformation
│   ├── s3_operations.py            # Handles S3 upload
│   ├── load_to_rds.py              # Loads data into RDS
├── main.py                 # Main ETL pipeline
├── .env                    # Environment variables (RDS credentials)
├── README.md               # Project documentation
```

## Installation & Setup

1. **Clone the repository**

   ```sh
   git clone https://github.com/your-repo/etl-pipeline.git
   cd etl-pipeline
   ```

2. **Install dependencies**

   ```sh
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   Create a `.env` file and add the following:

   ```sh
   RDS_DB=postgresql://username:password@hostname:port/database
   AWS_ACCESS_KEY_ID=your_access_key
   AWS_SECRET_ACCESS_KEY=your_secret_key
   ```

## Running the ETL Pipeline

Execute the pipeline with:

```sh
python main.py
```

## AWS S3 & RDS Integration

- **Uploads raw and transformed data** to an S3 bucket.
- **Loads processed data into RDS** using SQLAlchemy.
- **Final merged dataset** is stored as `combined_data.csv` in S3.

## Logging & Debugging

Logs are stored in `logs/etl_pipeline.log`. Check for errors and process tracking.

