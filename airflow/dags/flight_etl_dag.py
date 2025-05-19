from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from include.utils import *
import numpy as np
from airflow.exceptions import AirflowFailException
from include.logging_config import setup_logger


kaggle_url = "mahatiratusher/flight-price-dataset-of-bangladesh"
output_dir = "/usr/local/airflow/input"



logger = setup_logger(__name__)  # or setup_logger("etl.utils")

default_args = {
    'owner': 'gkessuman',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# DAG Tasks
def get_dataset():
    try:
        path = download_dataset(kaggle_url, output_dir)
        logger.info(f"Dataset downloaded to: {path}")
        return path
    except Exception as e:
        logger.error(f"Error in get_dataset: {e}")
        raise AirflowFailException("Dataset download failed")


def stage():
    try:
        df = pd.read_csv(f"{output_dir}/Flight_Price_Dataset_of_Bangladesh.csv")
        logger.info(f"Loaded dataset with shape: {df.shape}")

        df = prepare_data(df)

        host = os.environ.get("MYSQL_HOST", "mysql")
        user = os.environ.get("MYSQL_USER", "root")
        password = os.environ.get("MYSQL_PASSWORD", "my-secret")
        dbname = os.environ.get("MYSQL_DB", "bangladesh_flight_db")
        url = f"mysql+mysqlconnector://{user}:{password}@{host}/{dbname}"

        bangladesh_conn = get_connection_sqlalchemy(url)
        stage_to_mysql_alchemy(df, bangladesh_conn)

        logger.info("Data staged successfully to MySQL.")
    except Exception as e:
        logger.error(f"Error in stage: {e}")
        raise AirflowFailException("Staging data failed")

def transform(**context):
    try:
        host = os.environ.get("MYSQL_HOST", "mysql")
        user = os.environ.get("MYSQL_USER", "root")
        password = os.environ.get("MYSQL_PASSWORD", "my-secret")
        dbname = os.environ.get("MYSQL_DB", "bangladesh_flight_db")
        url = f"mysql+mysqlconnector://{user}:{password}@{host}/{dbname}"

        conn = get_connection_sqlalchemy(url)
        df = pd.read_sql("SELECT * FROM bangladesh_flight", conn)

        if "total_fare" not in df.columns:
            df['total_fare'] = df['tax_surcharge'] + df['base_fare']

        df['peak_season'] = np.where(df["seasonality"] != "Regular", "peaked", "not peaked")

        transformed_path = "/tmp/transformed_data.csv"
        df.to_csv(transformed_path, index=False)

        context['ti'].xcom_push(key='transformed_path', value=transformed_path)
        logger.info("Data transformed and saved to temporary file.")
    except Exception as e:
        logger.error(f"Error in transform: {e}")
        raise AirflowFailException("Transformation failed")
    
    
def load(**context):
    try:
        transformed_path = context['ti'].xcom_pull(task_ids='transform_data', key='transformed_path')
        df = pd.read_csv(transformed_path)

        pg_url = "postgresql+psycopg2://postgres:postgres@postgres/bangladesh_flight_final_db"
        pg_conn = get_connection_sqlalchemy(pg_url)

        df.to_sql("bangladesh_flight_analytics", pg_conn, if_exists='replace', index=False)
        logger.info("Loaded transformed data to PostgreSQL.")
    except Exception as e:
        logger.error(f"Error in load: {e}")
        raise AirflowFailException("Load to PostgreSQL failed")
    


    
with DAG(
    "my_own_dag",
    default_args=default_args
) as dag:
    pass

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=get_dataset,
    )

    stage_data = PythonOperator(
        task_id="stage_data",
        python_callable=stage,
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load,
    )

    extract_data >> stage_data >> transform_data >> load_data