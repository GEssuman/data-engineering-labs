from utils import *
import pandas as pd

host=os.environ.get("MYSQL_HOST")
user=os.environ.get("MSQL_USER")
password=os.environ.get("MYSQL_PASSWORD")
dbname = os.environ.get("MYSQL_DB")
MYSQL_URL=f"mysql+mysqlconnector://{user}:{password}@{host}/{dbname}"
POSTGRES_URL="postgresql+psycopg2://airflow:airflow@localhost:5432/mydatabase"




if __name__=="__main__":
    #connect ot mysql database
    connection = get_connection_sqlalchemy()

    # load the data in pandas dataframe
    flight_df = pd.read_table()

    #transform data
    if not ("total_fare" in flight_df.columns):
        flight_df['total_fare']=flight_df['tax_surcharge'] + flight_df['base_fare']


    

    