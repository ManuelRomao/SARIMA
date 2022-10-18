# Databricks notebook source
import numpy as np, pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose

# COMMAND ----------

# MAGIC %sql
# MAGIC use database thesis

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe sales_bronze;

# COMMAND ----------

(spark
  .readStream
  .table("sales_raw")
  .createOrReplaceTempView("tmp_sales_gold"))

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

(spark.table("sales_grouped_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "/mnt/tese.manuel.romao/checkPointGold/")
      .outputMode("complete")
      .trigger(once=True)
      .table("grouped_sales"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from grouped_sales;

# COMMAND ----------

pd_df = (
  spark
    .table('grouped_sales')
    ).toPandas()

# COMMAND ----------

pd_df.rolling

# COMMAND ----------

# MAGIC %python
# MAGIC print(grouped_sales.rolling(7).sum(7))

# COMMAND ----------

seasonal_decompose(spark.table("sales_raw").groupBy("date").sum("sales_qty"))
