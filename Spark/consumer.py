from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json,current_timestamp
from pyspark.sql.functions import expr,split,max,count,avg,min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import logging


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .getOrCreate()
    #.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.apache.kafka:kafka-clients:3.8.0") \
    #.config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:./log4j.properties") \


# Get the logger and set its level
log4jLogger = spark._jvm.org.apache.log4j
log4jLogger.LogManager.getLogger("org").setLevel(log4jLogger.Level.ERROR)
log4jLogger.LogManager.getLogger("akka").setLevel(log4jLogger.Level.ERROR)

# For disabling the logs completely, use Level.OFF
log4jLogger.LogManager.getRootLogger().setLevel(log4jLogger.Level.OFF)

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Topic_1") \
    .load()

# Cast the value column to string
kafka_df =  kafka_df.selectExpr("CAST(value AS STRING)")

split_cols = split(kafka_df['value'], ',') 

# Now applying split() using withColumn() 
df1 = kafka_df.withColumn('Product_category', split_cols.getItem(0).cast('STRING'))\
.withColumn('Product_name', split_cols.getItem(1).cast("STRING"))\
.withColumn('Product_price', split_cols.getItem(2).cast("float"))\
.withColumn('event_time', current_timestamp()) #create a timestamp

df1 = df1.drop("value")

# Add watermark on the generated timestamp column
#df1 = df1.withWatermark("event_time", "1 minutes")


df2 = df1.select("Product_category", "Product_price")\
.groupby("Product_category")\
.agg(max("Product_price").alias("max price"),min("Product_price").alias("min price"))

""" df3 = df1.select("Product_category")\
.groupby("Product_category")\
.agg(count("Product_category").alias("total number of product"))

df4 = df1.select("Product_category", "Product_price")\
.groupby("Product_category")\
.agg(avg("Product_price").alias("avg price each product")) """

# Write the parsed JSON data to the console (or any other sink)
""" query1 = df1.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate","false")\
    .option("checkpointLocation", "checkpoint") \
    .start() """
    
    #.option("path", "output") \
    #.start()

query2 = df2.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate","false")\
    .option("checkpointLocation", "checkpoint13")\
    .start()
    #.option("checkpointLocation", "checkpoint2") \
    #.option("failOnDataloss","false")\
    

# outputmode("compelete") with aggregtion mode

""" query3 = df3.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate","false")\
    .option("checkpointLocation", "checkpoint") \
    .option("failOnDataloss","false")\
    .start()

query4 = df4.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate","false")\
    .option("checkpointLocation", "checkpoint") \
    .option("failOnDataloss","false")\
    .start() """

# Wait for the query to process for 10 seconds
#query1.awaitTermination(10)
query2.awaitTermination(10)
#query3.awaitTermination(10)
#query4.awaitTermination(10)