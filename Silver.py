# Databricks notebook source
# MAGIC %sql
# MAGIC use database thesis

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from sales_raw;

# COMMAND ----------

spark.table(tableName="sales_bronze").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view tmp_sales_bronze as (
# MAGIC select *, current_timestamp() as ingestion_date, input_file_name() as file_name
# MAGIC from bronze_sales);
# MAGIC 
# MAGIC select * from tmp_sales_bronze;

# COMMAND ----------


