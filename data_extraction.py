from utils import *
from logging_config import setup_logger
import os
import pandas as pd

kaggle_url = "mahatiratusher/flight-price-dataset-of-bangladesh"
output_dir = "./input"


logger = setup_logger(__name__) 

def extract():
    logger.info("Running extract_data")
    path = download_dataset(kaggle_url, output_dir)
    logger.info(f"Dataset downloaded and prepared. Directory:- {path}")


def stage_data(connection, df):
    logger.info("Staging Data to MySQL Database")
    stage_to_mysql_alchemy(df, connection)
    logger.info("Staging Complete")

if __name__=="__main__":

    host=os.environ.get("MYSQL_HOST")
    user=os.environ.get("MSQL_USER")
    password=os.environ.get("MYSQL_PASSWORD")
    dbname = os.environ.get("MYSQL_DB")
    URL=f"mysql+mysqlconnector://{user}:{password}@{host}/{dbname}" 
    extract()

    connection = get_connection_sqlalchemy(URL)
    flight_df = pd.read_csv(f"{output_dir}/Flight_Price_Dataset_of_Bangladesh.csv")

    prepare_data(flight_df)

    stage_data(connection, flight_df)
    

