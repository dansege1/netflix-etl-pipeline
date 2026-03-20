System Architecture
The pipeline follows a functional Medallion Architecture (Bronze to Silver) to ensure data integrity and scalability.

Data Lake (MinIO/S3): Raw Netflix CSV data is stored as unstructured objects in a MinIO bucket, simulating a cloud-native S3 environment.

Orchestration (Apache Airflow): A dedicated DAG manages the workflow, handling task dependencies, retries, and monitoring.

Transformation (Python/Pandas): * Standardized date_added to ISO-8601 format.

Implemented categorical imputation for missing director and cast data.

Cleaned and filtered records for downstream analytics.

Warehouse (PostgreSQL): Processed data is loaded into a structured netflix_titles table using SQLAlchemy for high-performance inserts.

🛠 Tech Stack
Language: Python 3.x

Libraries: Pandas, SQLAlchemy, Boto3, Psycopg2

Orchestration: Apache Airflow

Infrastructure: Docker & Docker Compose

Storage: MinIO (S3-compatible), PostgreSQL

🚀 Key Technical Challenges Solved
Container Communication: Engineered a custom Docker bridge network to allow seamless communication between Airflow, MinIO, and Postgres containers.

Schema Evolution: Handled PostgreSQL null constraints and data type mapping from Python to SQL.

Environment Security: Implemented .env variable management to keep database credentials secure and out of the source code.
