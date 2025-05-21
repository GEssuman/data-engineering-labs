from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from include.utils import *
import numpy as np
from airflow.exceptions import AirflowFailException, AirflowSkipException
from include.logging_config import setup_logger
from sqlalchemy.exc import OperationalError, IntegrityError


kaggle_url = "mahatiratusher/flight-price-dataset-of-bangladesh"
output_dir = "/usr/local/airflow/input"

mysql_host = os.environ.get("MYSQL_HOST", "mysql")
mysql_user = os.environ.get("MYSQL_USER", "root")
mysql_password = os.environ.get("MYSQL_PASSWORD", "my-secret")
mysql_dbname = os.environ.get("MYSQL_DB", "bangladesh_flight_db")
mysql_url = f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_dbname}"


postgres_host = os.environ.get("POSTGREST_HOST", "postgres")
postgres_user = os.environ.get("POSTGRES_USER", "postgres")
postgres_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
postgres_dbname = os.environ.get("POSTGRES_DB", "bangladesh_flight_final_db")
postgres_url = f"postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}/{postgres_dbname}"



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
        logger.info("Starting staging process...")
        df = pd.read_csv(f"{output_dir}/Flight_Price_Dataset_of_Bangladesh.csv")
        logger.info(f"Loaded dataset with shape: {df.shape}")

        df = prepare_data(df)

        mysql_conn = get_connection_sqlalchemy(mysql_url)
        stage_to_mysql_alchemy(df, mysql_conn)

        logger.info("Data staged successfully to MySQL.")

    except IntegrityError as ie:
        logger.error(f"Integrity error during staging: {ie}")
        raise AirflowSkipException("Data integrity issue — skipping.")
    
    except OperationalError as oe:
        logger.warning(f"Temporary DB error: {oe} — will retry.")
        raise oe
    
    except Exception as e:
        logger.error("Staging failed.")
        raise AirflowFailException(str(e))

def transform(**context):
    try:
        logger.info("Beginning transformation process...")
        mysql_conn = get_connection_sqlalchemy(mysql_url)

        # mysql_conn = get_connection_sqlalchemy(url)
        df = pd.read_sql("SELECT * FROM bangladesh_flight", mysql_conn)
        logger.info(f"Loaded data for transformation: {df.shape}")


        if "total_fare" not in df.columns:
            df['total_fare'] = df['tax_surcharge'] + df['base_fare']

        df['peak_season'] = np.where(df["seasonality"] != "Regular", "peaked", "not peaked")
        
        transformed_path = "/tmp/transformed_data.csv"
        df.to_csv(transformed_path, index=False)
        context['ti'].xcom_push(key='transformed_path', value=transformed_path)
        

        # Save and push other aggregations
        aggregations = {
            "avg_fare_per_airline": avg_fare_by_airline(df), # Average Fare Per Airline
            "peak_season_summary": peak_season_agg(df), # Peak Season Summary
            "top_source_dest_pairs": source_dest_agg(df), # 3 Top Source-Destination Pairs
        }
       
        for name, agg_df in aggregations.items():
            file_path = f"/tmp/{name}.csv"
            agg_df.to_csv(file_path, index=False)
            context['ti'].xcom_push(key=f"{name}_csv", value=file_path)

        logger.info("Transformation complete.")

    except Exception as e:
        logger.exception("Transformation failed.")
        raise AirflowFailException(str(e))
    
    
def load_csv_to_postgres(key, table_name, **context):
    try:
        logger.info(f"Starting load to {table_name}...")
        file_path = context['ti'].xcom_pull(task_ids='transform_data', key=key)
        df = pd.read_csv(file_path)

        pg_conn = get_connection_sqlalchemy(postgres_url)
        df.to_sql(table_name, pg_conn, if_exists='append', index=False)
        
        logger.info(f"Successfully loaded {len(df)} records into '{table_name}'.")

    except IntegrityError as ie:
        logger.error(f"Integrity error during load: {ie}")
        raise AirflowSkipException("Integrity issue — skipping.")
    
    except OperationalError as oe:
        logger.warning(f"Database connection issue during load: {oe}")
        raise oe
    
    except Exception as e:
        logger.exception(f"Load to {table_name} failed.")
        raise AirflowFailException(str(e))

# def load_avg_fare_per_airline(**context):
#     try:
#         avg_fare_per_airline_csv = context['ti'].xcom_pull(task_ids='transform_data', key='avg_fare_per_airline_csv')
#         df = pd.read_csv(avg_fare_per_airline_csv)

#         pg_conn = get_connection_sqlalchemy(postgres_url)

#         df.to_sql("avg_fare_per_airline", pg_conn, if_exists='append', index=False)
#         logger.info("Loaded transformed data to PostgreSQL.")
#     except Exception as e:
#         logger.error(f"Error in load: {e}")
#         # raise AirflowFailException("Load to PostgreSQL failed")
    

# def load_peak_season_summary(**context):
#     try:
#         peak_season_summary_csv = context['ti'].xcom_pull(task_ids='transform_data', key='peak_season_summary_csv')
#         df = pd.read_csv(peak_season_summary_csv)

#         pg_conn = get_connection_sqlalchemy(postgres_url)

#         df.to_sql("peak_season_summary", pg_conn, if_exists='append', index=False)
#         logger.info("Loaded transformed data to PostgreSQL.")
#     except Exception as e:
#         logger.error(f"Error in load: {e}")
#         # raise AirflowFailException("Load to PostgreSQL failed")


# def load_top_source_dest_pairs(**context):
#     try:
#         top_source_dest_pairs_csv = context['ti'].xcom_pull(task_ids='transform_data', key='top_source_dest_pairs_csv')
#         df = pd.read_csv(top_source_dest_pairs_csv)

#         pg_conn = get_connection_sqlalchemy(postgres_url)

#         df.to_sql("top_source_dest_pairs", pg_conn, if_exists='append', index=False)
#         logger.info("Loaded transformed data to PostgreSQL.")
#     except Exception as e:
#         logger.error(f"Error in load: {e}")
#         # raise AirflowFailException("Load to PostgreSQL failed")
    
with DAG(
    "my_own_dag",
    default_args=default_args,
    catchup=False,
    description="ETL pipeline for Bangladesh flight data",
    tags=["ETL", "flight"],
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
        task_id="load_transformed_data_to_postgres",
        python_callable=load_csv_to_postgres,
        op_kwargs={"key": "transformed_path", "table_name": "bangladesh_flight_analytics"},
    )

    load_avg_fare_per_airline = PythonOperator(
        task_id="load_avg_fare_per_airline_to_postgres",
        python_callable=load_csv_to_postgres,
        op_kwargs={"key": "avg_fare_per_airline_csv", "table_name": "avg_fare_per_airline"},
    )

    load_peak_season_summary = PythonOperator(
        task_id="load_peak_season_summary_to_postgres",
        python_callable=load_csv_to_postgres,
        op_kwargs={"key": "peak_season_summary_csv", "table_name": "peak_season_summary"},
    )

    load_top_source_dest_pairs = PythonOperator(
        task_id="load_top_source_dest_pairse_to_postgres",
        python_callable=load_csv_to_postgres,
        op_kwargs={"key": "top_source_dest_pairs_csv", "table_name": "top_source_dest_pairs"},
    )

    extract_data >> stage_data >> transform_data >> [
        load_data,load_avg_fare_per_airline, 
        load_peak_season_summary, 
        load_top_source_dest_pairs
    ]