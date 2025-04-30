import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType



#Create a local SparkSession
spark = SparkSession.builder.appName("RealTimeEventSreaming").getOrCreate()

Product_View_Event_Schema = StructType([ 
    StructField('event_type', StringType(), True), 
    StructField('product_name', StringType(), True), 
    StructField('customer_name', StringType(), True), 
    StructField('date_viewed', StringType()(), True)
]) 

# //


product_view_events = spark \
    .readStream \
    .schema(Product_View_Event_Schema) \
    .csv("../../e-commerce-user-events/product-view-events")


query = product_view_events \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='1 second') \
    .start()

query.awaitTermination()