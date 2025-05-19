from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from include.utils import *
import numpy as np
from airflow.models import Variable
import pickle


kaggle_url = "mahatiratusher/flight-price-dataset-of-bangladesh"
output_dir = "/usr/local/airflow/input"





default_args = {
    'owner': 'gkessuman',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
def serialize_df(df):
    return pickle.dumps(df)

def deserialize_df(binary_data):
    return pickle.loads(binary_data)

def get_dataset():
    path = download_dataset(kaggle_url, output_dir)
    print(f"The dir for the datasest-:{path}")
    return path


def stage():
    df = pd.read_csv(f"{output_dir}/Flight_Price_Dataset_of_Bangladesh.csv")
    print("kojo",df)

    df = prepare_data(df)
    print(df)
    host=os.environ.get("MYSQL_HOST", "mysql")
    user=os.environ.get("MYSQL_USER", "root")
    password=os.environ.get("MYSQL_PASSWORD", "my-secret")
    dbname = os.environ.get("MYSQL_DB", "bangladesh_flight_db")
    url=f"mysql+mysqlconnector://{user}:{password}@{host}/{dbname}"
    bangladesh_conn = get_connection_sqlalchemy(url)
    stage_to_mysql_alchemy(df, bangladesh_conn)

def transform(**context):
    # read from mysql
    host = os.environ.get("MYSQL_HOST", "mysql")
    user = os.environ.get("MYSQL_USER", "root")
    password = os.environ.get("MYSQL_PASSWORD", "my-secret")
    dbname = os.environ.get("MYSQL_DB", "bangladesh_flight_db")
    url = f"mysql+mysqlconnector://{user}:{password}@{host}/{dbname}"
    conn = get_connection_sqlalchemy(url)

    df = pd.read_sql("SELECT * FROM bangladesh_flight", conn)

    # tranform
    if not ("total_fare" in df.columns):
        df['total_fare']=df['tax_surcharge'] + df['base_fare']
    df['peak_season'] = np.where(df["seasonality"] != "Regular", "peaked", "not peaked")


    # Push via XCom
    # Save to CSV or temp table
    transformed_path = "/tmp/transformed_data.csv"
    df.to_csv(transformed_path, index=False)

    # Push file path to XCom
    context['ti'].xcom_push(key='transformed_path', value=transformed_path)

    
    
def load(**context):
    # Pull path from XCom
    transformed_path = context['ti'].xcom_pull(task_ids='transform_data', key='transformed_path')
     # Load the CSV
    df = pd.read_csv(transformed_path)


    pg_url = f"postgresql+psycopg2://postgres:postgres@postgres/bangladesh_flight_final_db"
    pg_conn = get_connection_sqlalchemy(pg_url)
   
   
    #load to postgress
    df.to_sql("bangladesh_flight_analytics", pg_conn, if_exists='replace', index=False)
    print("Loaded transformed data to PostgreSQL.")

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