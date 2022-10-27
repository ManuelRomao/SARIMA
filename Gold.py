# Databricks notebook source
# MAGIC %sql
# MAGIC use database thesis

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from grouped_sales
# MAGIC -- where year(date) = 2021;

# COMMAND ----------

# MAGIC %run ./Bronze&Silver(toChange)

# COMMAND ----------

import numpy as np, pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose

# COMMAND ----------

# MAGIC %sql
# MAGIC use database thesis

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe sales_bronze;

# COMMAND ----------

query_tmp_gold = (spark
                        .readStream
                        .table("sales_raw")
                        .createOrReplaceTempView("tmp_sales_gold"))
query_tmp_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view sales_grouped_tmp as (
# MAGIC with sales_aux as (
# MAGIC select 
# MAGIC   date
# MAGIC   , sum(sales_qty) as grouped_sales
# MAGIC from tmp_sales_gold
# MAGIC group by date
# MAGIC order by date)
# MAGIC select 
# MAGIC   date
# MAGIC   , grouped_sales
# MAGIC   , sum(grouped_sales) over (order by date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) as rolling_5days_sales
# MAGIC   , lag((sum(grouped_sales) over (order by date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)), -5, 0) over (order by date) as lagged_cum_sales
# MAGIC from sales_aux);
# MAGIC 
# MAGIC select * from sales_grouped_tmp;

# COMMAND ----------

query_gold = (spark.table("sales_grouped_tmp")
                    .writeStream
                    .format("delta")
                    .option("checkpointLocation", "/mnt/tese.manuel.romao/checkPoint/")
                    .outputMode("complete")
                    .trigger(once=True)
                    .table("grouped_sales"))

query_gold.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from grouped_sales;

# COMMAND ----------

# Stop Streams
for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()
