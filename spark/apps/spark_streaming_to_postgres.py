import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType
import os


SPARK_WORKDIR = os.getenv('SPARK_WORKDIR')
POSTGRES_USER= os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD= os.getenv('POSTGRES_PASSWORD')


url = f"jdbc:postgresql://postgres_db:5432/realtime_event_db"

properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}
#Create a local SparkSession
spark = SparkSession.builder.appName("RealTimeEventSreaming")\
    .config("spark.jars", "/opt/real-time-spark/spark/resources/postgresql-42.7.2.jar") \
    .config("spark.driver.extraClassPath", "/opt/real-time-spark/spark/resources/postgresql-42.7.2.jar") \
    .config("spark.executor.extraClassPath", "/opt/real-time-spark/spark/resources/postgresql-42.7.2.jar") \
    .getOrCreate()

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


product_view_events = spark \
    .readStream \
    .option("header", "true") \
    .schema(Product_View_Event_Schema) \
    .csv("../../e-commerce-user-events/product-view-events")

product_purchase_events = spark \
    .readStream \
    .option("header", "true") \
    .schema(Product_Purchase_Event_Schema) \
    .csv("../../e-commerce-user-events/product-purchase-events")

product_view_events = product_view_events.withColumn("date_viewed", 
                                                    F.to_timestamp(F.col("date_viewed"), "dd-MM-yyyy, HH:mm:ss"))
product_view_events = product_view_events.withColumnRenamed("date_viewed", 'event_date')
product_view_events = product_view_events.withColumn("customer_name", 
                                                     F.concat(F.col("customer_surname"), F.lit(" "), F.col("customer_firstname")))
product_view_events = product_view_events.drop('customer_surname', 'customer_firstname')





product_purchase_events = product_purchase_events.withColumn("date_purchased", 
                                                    F.to_timestamp(F.col("date_purchased"), "dd-MM-yyyy, HH:mm:ss"))
product_purchase_events = product_purchase_events.withColumnRenamed("date_purchased", "event_date")
product_purchase_events = product_purchase_events.withColumn("customer_name", 
                                                     F.concat(F.col("customer_surname"), F.lit(" "), F.col("customer_firstname")))
product_purchase_events = product_purchase_events.drop('customer_surname', 'customer_firstname')


# view_event_query = product_view_events \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .trigger(processingTime='1 second') \
#     .start()

# purchase_event_query = product_purchase_events \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .trigger(processingTime='1 second') \
#     .start()


def write_to_postgres(batch_df, batch_id):
    batch_df.write.jdbc(
        url=url,
        table="event_log",
        mode="append",
        properties=properties
    )






purchase_event_query = product_purchase_events.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

view_event_query = product_view_events.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()


view_event_query.awaitTermination()
purchase_event_query.awaitTermination()