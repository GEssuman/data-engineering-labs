import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType



#Create a local SparkSession
spark = SparkSession.builder.appName("RealTimeEventSreaming").getOrCreate()

Product_View_Event_Schema = StructType([ 
    StructField('event_type', StringType(), True), 
    StructField('product_name', StringType(), True), 
    StructField('customer_surname', StringType(), True), 
    StructField('customer_firstname', StringType(), True), 
    StructField('date_viewed', StringType(), True)
]) 


Product_Purchase_Event_Schema = StructType([ 
    StructField('event_type', StringType(), True), 
    StructField('product_name', StringType(), True), 
    StructField('customer_surname', StringType(), True), 
    StructField('customer_firstname', StringType(), True), 
    StructField('date_purchase', StringType(), True),
    StructField('unit_cost', StringType(), True),
    StructField('quanity', StringType(), True)
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
product_view_events = product_view_events.withColumnRenamed("date_viewed", 'event_time')
product_view_events = product_view_events.withColumn("customer_name", 
                                                     F.concat(F.col("customer_surname"), F.lit(" "), F.col("customer_firstname")))
product_view_events = product_view_events.drop('customer_surname', 'customer_firstname')





product_purchase_events = product_purchase_events.withColumn("date_purchase", 
                                                    F.to_timestamp(F.col("date_purchase"), "dd-MM-yyyy, HH:mm:ss"))
product_purchase_events = product_purchase_events.withColumnRenamed("date_purchase", "event_time")
product_purchase_events = product_purchase_events.withColumn("customer_name", 
                                                     F.concat(F.col("customer_surname"), F.lit(" "), F.col("customer_firstname")))
product_purchase_events = product_purchase_events.drop('customer_surname', 'customer_firstname')


view_event_query = product_view_events \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='1 second') \
    .start()

purchase_event_query = product_purchase_events \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='1 second') \
    .start()

view_event_query.awaitTermination()
purchase_event_query.awaitTermination()