# Databricks notebook source
# MAGIC %sql 
# MAGIC create database if not exists Thesis

# COMMAND ----------

# MAGIC %sql
# MAGIC use database thesis

# COMMAND ----------

# pyspark functions
from pyspark.sql.functions import *

# url processing
import urllib

# COMMAND ----------

# file location and type
file_location = "/mnt/tese.manuel.romao/data/"
file_type = "csv"

infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

#Read the stream
# df = spark.read.format(file_type)\
#     .option("inferSchema", infer_schema)\
#     .option("header", first_row_is_header)\
#     .option("sep", delimiter)\
#     .load(file_location)

# df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Read the stream
# MAGIC -- create table if not exists bronze_sales
# MAGIC create or replace table bronze_sales
# MAGIC -- (year string
# MAGIC -- , date string
# MAGIC -- , unit string
# MAGIC -- , store string
# MAGIC -- , direction string
# MAGIC -- , product string
# MAGIC -- , sales string
# MAGIC -- , sales_qty string)
# MAGIC (year int
# MAGIC , date date
# MAGIC , unit string
# MAGIC , store string
# MAGIC , direction string
# MAGIC , product string
# MAGIC , sales double
# MAGIC , sales_qty int)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_sales
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe bronze_sales

# COMMAND ----------

# from pyspark.sql.types import *

# cvsSchema = StructType([StructField("year", IntegerType(), True),
#                        StructField("date", DateType(), True),
#                        StructField("unit", StringType(), True),
#                        StructField("store", StringType(), True),
#                        StructField("direction", StringType(), True),
#                        StructField("product", StringType(), True),
#                        StructField("sales", DoubleType(), True),
#                        StructField("sales_qty", DoubleType(), True)])

# COMMAND ----------

# def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
#     query = (spark.readStream
#                   .format("cloudFiles")
#                   .option("cloudFiles.format", source_format)
#                   .option("cloudFiles.schemaHint", "year INT, date DATE, sales double, sales_qty double")
#                   .option("cloudFiles.schemaLocation", checkpoint_directory)
#                   .option("header", "true")
#                   .load(data_source)
#                   .writeStream
#                   .option("checkpointLocation", checkpoint_directory)
#                   .option("mergeSchema", "true")
#                   .table(table_name))
#     return query

# COMMAND ----------

# def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
#     query = (spark.readStream
#                   .format("cloudFiles")
#                   .option("cloudFiles.format", source_format)
# #                   .option("cloudFiles.schemaHint", "year INT, date DATE, sales double, sales_qty double")
#                   .option("cloudFiles.schemaLocation", checkpoint_directory)
#                   .option("cloudFiles.useNotifications", "true")
# #                   .option("header", "true")
#                   .load(data_source)
#                   .createOrReplaceTempView(table_name))
#     return query

# COMMAND ----------

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    querySalesStreaming = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaHint", "year INT, date DATE, sales double, sales_qty double")
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .option("cloudFiles.validateOptions", "false")
                  .option("header", "true")
                  .load(data_source)
                  .createOrReplaceTempView(table_name))
    return querySalesStreaming

# COMMAND ----------

# tmp_sales_ingestion = autoload_to_table(data_source = "/mnt/tese.manuel.romao/data/",
#                           source_format = "csv",
#                           table_name = "streaming_tmp_bronze_sales",
#                           checkpoint_directory = "/mnt/tese.manuel.romao/checkPoints/")

# COMMAND ----------

autoload_to_table(data_source = "/mnt/tese.manuel.romao/data/",
                          source_format = "csv",
                          table_name = "streaming_tmp_sales_raw",
                          checkpoint_directory = "/mnt/tese.manuel.romao/checkPoints/")

# COMMAND ----------

spark.table("streaming_tmp_sales_raw").display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe streaming_tmp_sales_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view tmp_sales_raw as (
# MAGIC select 
# MAGIC   cast(year as int)
# MAGIC   , cast (date as date)
# MAGIC   , unit
# MAGIC   , store
# MAGIC   , direction
# MAGIC   , product
# MAGIC   , cast(sales as double)
# MAGIC   , cast(sales_qty as int)
# MAGIC   , _rescued_data
# MAGIC   , input_file_name() source_file
# MAGIC   , current_timestamp() receipt_time
# MAGIC from streaming_tmp_sales_raw);
# MAGIC 
# MAGIC -- select * from tmp_sales_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_sales_raw;

# COMMAND ----------

(spark.table("tmp_sales_raw")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/tese.manuel.romao/checkPoints/")
    .outputMode("append")
    .queryName("salesAppend")
    .trigger(once=True)
    .table("sales_raw")
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- select * from sales_raw;
# MAGIC select distinct source_file from sales_raw;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view vw_sales_bronze as (
# MAGIC select *, current_timestamp() as ingestion_date, input_file_name() as ingestion_file from streaming_tmp_bronze_sales);
# MAGIC 
# MAGIC select * from vw_sales_bronze;

# COMMAND ----------

spark.table("vw_sales_bronze").display()

# COMMAND ----------

(spark.table("vw_sales_bronze")                               
    .writeStream                                                
    .option("checkpointLocation", "/mnt/tese.manuel.romao/checkPoints/")
    .outputMode("append")
    .queryName("salesAppend")
    .trigger(once=True)
    .table("sales_bronze")
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe test

# COMMAND ----------

display(tmp_sales_ingestionquery)

# COMMAND ----------

test = (query
       .withColumn("file", input_file_name()))
display(test)

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

def block_until_stream_is_ready(query, min_batches=2):
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")

block_until_stream_is_ready(query)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

(spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaHints", "year INT, date DATE")
      .option("cloudFiles.schemaLocation", "/mnt/tese.manuel.romao/data/")
      .option("checkpointLocation", "/mnt/tese.manuel.romao/checkPoints/")
      .load("/mnt/tese.manuel.romao/data/")
      .createOrReplaceTempView("tmp_sales_raw"))


display(spark.table("tmp_sales_raw"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe tmp_sales_raw;

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

spark.table("vw_sales_ingestion").display()

# COMMAND ----------

(spark.table("vw_sales_ingestion")                               
    .writeStream
    .option("checkpointLocation", "/mnt/tese.manuel.romao/checkPoints/")
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(once=True)
    .table("bronze_sales")
    .awaitTermination() # This optional method blocks execution of the next cell until the incremental batch write has succeeded
)

spark.table("bronze_sales").display()

# COMMAND ----------

spark.table("bronze_sales").display()

# COMMAND ----------

streamQuery = 

# COMMAND ----------

# Stop Streams
for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()
