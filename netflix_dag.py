from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
from io import BytesIO
from sqlalchemy import create_engine, text

# --- CONFIGURATION ---
MINIO_ENDPOINT = "http://minio:9000"
S3_KEY = "bx3htBVoAFJ7ZOn8B5bO"
S3_SECRET = "cLZjbEDXtXpb7Xql2FBDFQi47EZChmih2aWyWDmV"
DB_URL = "postgresql://postgres:********@postgres_container:5432/netflix_db"

# --- ETL LOGIC ---
def run_netflix_etl():
    # 1. Extract
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, 
                      aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET)
    response = s3.get_object(Bucket='netflix-data', Key='netflix_titles.csv')
    df = pd.read_csv(BytesIO(response['Body'].read()))
    
    # 2. Transform
    df['date_added'] = pd.to_datetime(df['date_added'].str.strip())
    df[['director', 'cast', 'country']] = df[['director', 'cast', 'country']].fillna('Unknown')

    
    def validate_netflix_data(df):
    """
    Professional Data Quality Checks
    """
    #  Check if the dataframe is empty
    if df.empty:
        raise ValueError("Data Quality Check Failed: The source file is empty!")

    #  Check for duplicate show_ids
    if df['show_id'].duplicated().any():
        raise ValueError("Data Quality Check Failed: Duplicate show_ids detected!")

    # Logic Check: Release year shouldn't be in the future
    current_year = 2026 
    if (df['release_year'] > current_year).any():
        print("Warning: Some titles have future release years. Filtering them out.")
        df = df[df['release_year'] <= current_year]

    print("Data Quality Checks Passed! ✅")
    return df
    
    # 3. Load
    engine = create_engine(DB_URL)
    df.to_sql('netflix_titles', engine, if_exists='replace', index=False)
    print("ETL Job Completed Successfully!")

# --- DAG DEFINITION ---
default_args = {
    'owner': 'Oluwasegun',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'netflix_minio_to_postgres',
    default_args=default_args,
    description='ETL pipeline for Netflix data',
    schedule_interval= '@daily',  # Change to '@daily' for automation
    start_date=datetime(2026, 3, 19),
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id='run_full_etl',
        python_callable=run_netflix_etl,
    )
