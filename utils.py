import kagglehub
import os
import shutil
import pathlib
from logging_config import setup_logger
from sqlalchemy import create_engine
import pandas as pd
import mysql.connector
from mysql.connector.errors import IntegrityError as IntegrityErrorSQLConn
from sqlalchemy.exc import IntegrityError as IntegrityErrorAlchemy
import re



logger = setup_logger(__name__)  # or setup_logger("etl.utils")


def create_dir(dir):
    try:
        os.makedirs(dir)
        logger.info(f"Created directory: {dir}")
    except FileExistsError as e:
        logger.warning(f"the dir:- {dir} already exists")
    except Exception as e:
        logger.error(f"Error:- {e}")

def path_exists(dir):
    return os.path.exists(dir)


def download_dataset(url, output_dir):
    try:
        # Download latest version
        logger.info("Starting dataset download function.")

        if path_exists(output_dir) and os.listdir(output_dir):
            logger.info(f"Dataset already exists in '{output_dir}'. Skipping download.")
            return
        
        logger.info(f"Downloading dataset from: {url}")
        path = kagglehub.dataset_download(url)


        create_dir(output_dir)

        #move content in path to input path
        for file in os.listdir(path):
            src = os.path.join(path, file)
            shutil.move(src, output_dir)
        logger.info(f"Moved files: {path} → {output_dir}")

        
        shutil.rmtree(path)
        logger.info(f"Removed temporary download directory: {path}")
    except FileExistsError as e:
        logger.warning(f"File Already Exist:- {e}")
    except Exception as e:
        logger.error(f"Error:- {e}")
    finally:    
        return output_dir


def get_connection_sqlalchemy(url):
    try:
        safe_url = re.sub(r'//([^:]+):([^@]+)@', r'//\1:***@', url)
        logger.info(f"Connecting to the Database Server: {safe_url}")
        connection =  create_engine(url)
        logger.info(f"Database Connected Successfuly:- {url}")
        return connection
    except Exception as e:
        logger.error(f"Unable to connect to the Database: {e} ")



def get_connection_mysql(host, user, password, dbname):
    try:
        url=f"mysql+mysqlconnector://{user}:***@{host}/{dbname}"
        logger.info(f"Connecting to the Database Server: {url}")
        connection = mysql.connector.connect(
            host={host},
            user={user},
            password={password},
            database={dbname}
            )
        logger.info(f"Database Connected Successfuly:- {url}")
        return connection
    except Exception as e:
        logger.error(f"Unable to connect to the Database: {e} ")


def prepare_data(df):
    logger.info("Preapring data for Staging")
    column_mapping = {
        "Airline": "airline",
        "Source": "source",
        "Source Name": "source_name",
        "Destination": "destination",
        "Destination Name": "destination_name",
        "Departure Date & Time": "departure_datetime",
        "Arrival Date & Time": "arrival_datetime",
        "Duration (hrs)": "duration",
        "Stopovers": "stopovers",
        "Aircraft Type": "aircraft_type",
        "Class": "class",
        "Booking Source": "booking_source",
        "Base Fare (BDT)": "base_fare",
        "Tax & Surcharge (BDT)": "tax_surcharge",
        "Total Fare (BDT)": "total_fare",
        "Seasonality": "seasonality",
        "Days Before Departure": "days_before_departure"
    }
    df.rename(columns=column_mapping, inplace=True)

    df['departure_datetime'] = pd.to_datetime(df['departure_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['arrival_datetime'] = pd.to_datetime(df['arrival_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')

def stage_to_mysql_alchemy(df, conn):
    try:
        logger.info("Staging data to MYSQL Database")

        df.to_sql("bangladesh_flight", conn, if_exists="append",index=False)
        succes_count += 1
        logger.info(f"Staging completed")
    except IntegrityErrorAlchemy as e:
        logger.warning(f"Encounted Duplicate Entry : {e}")
    except Exception as e:
        logger.error(f" Error: {e}")

def stage_to_mysql_connector(df, conn):
    cursor = conn.cursor()

    # Assuming your column order matches
    sql = """
        INSERT INTO bangladesh_flight (
            airline, source, source_name, destination, destination_name,
            departure_datetime, arrival_datetime, duration, stopovers,
            aircraft_type, class, booking_source, base_fare, tax_surcharge,
            total_fare, seasonality, days_before_departure
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Convert dataframe to list of tuples
    try:
        data = df[[
            'airline', 'source', 'source_name', 'destination', 'destination_name',
            'departure_datetime', 'arrival_datetime', 'duration', 'stopovers',
            'aircraft_type', 'class', 'booking_source', 'base_fare', 'tax_surcharge',
            'total_fare', 'seasonality', 'days_before_departure'
        ]].values.tolist()
        logger.info("Staging data to MYSQL Database")
        cursor.executemany(sql, data)
        logger.info("Staged Succesfully")
        conn.commit()
        conn.close()
    except IntegrityErrorSQLConn as e:
        logger.warning(f"Encounted Duplicate Entry : {e}")
    except Exception as e:
        logger.error(f" Error: {e}")