import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType
import os
from utils import *


SPARK_WORKDIR = os.getenv('SPARK_WORKDIR')
POSTGRES_USER= os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD= os.getenv('POSTGRES_PASSWORD')


output_sink = {
    'url':f"jdbc:postgresql://postgres_db:5432/realtime_event_db",

    'properties': {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
    }

}


product_purchase_events_csv_dir ="../../e-commerce-user-events/product-purchase-events"
product_view_events_csv_dir ="../../e-commerce-user-events/product-view-events"
#Create a local SparkSession
spark = createSparkSession("RealTimeEventSreaming")


Product_View_Event_Schema = StructType([ 
    StructField('event_type', StringType(), True), 
    StructField('product_name', StringType(), True),
    StructField('unit_price', FloatType(), True), 
    StructField('customer_surname', StringType(), True), 
    StructField('customer_firstname', StringType(), True), 
    StructField('date_viewed', StringType(), True)
]) 

Product_Purchase_Event_Schema = StructType([ 
    StructField('event_type', StringType(), True), 
    StructField('product_name', StringType(), True), 
    StructField('customer_surname', StringType(), True), 
    StructField('customer_firstname', StringType(), True), 
    StructField('date_purchased', StringType(), True),
    StructField('unit_price', FloatType(), True),
    StructField('quantity', IntegerType(), True)
]) 


product_purchase_events =  subscribe_csv_stream(spark, Product_Purchase_Event_Schema, product_purchase_events_csv_dir)
product_view_events =  subscribe_csv_stream(spark, Product_View_Event_Schema, product_view_events_csv_dir)


transformed_product_view_events = transform_data(product_view_events, "view")
transformed_product_purchase_events = transform_data(product_purchase_events, "purchase")


purchase_event_query = writeStream(transformed_product_purchase_events, output_sink)
view_event_query = writeStream(transformed_product_view_events, output_sink)


view_event_query.awaitTermination()
purchase_event_query.awaitTermination()