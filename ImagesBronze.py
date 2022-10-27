# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *

# url processing
import urllib

# COMMAND ----------

# df = spark.read.format("image").load("/mnt/tese.manuel.romao.images/images/")
# display(df)

# COMMAND ----------

#%fs
# dbutils.fs/Tables.put("dbfs:/mnt/tese.manuel.romao.images/images/ManuelRomao.jpg", "This is the actual text that will be saved to disk. Like a 'Hello world!' example")
# FileStore/tables

# COMMAND ----------

# image_df = spark.read.format("image").load("dbfs:/mnt/tese.manuel.romao.images/images/ManuelRomao.jpg")

# display(image_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ![GrabNGoInfo Logo](files/images/ManuelRomao.jpg)

# COMMAND ----------

#%fs ls FileStore/tables

# COMMAND ----------

# display(image_df.select("image.origin"))

# COMMAND ----------

# displayHTML("<img src ='dbfs:/mnt/tese.manuel.romao.images/images/ManuelRomao.jpg'>")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ![my_test_image]("dbfs:/mnt/tese.manuel.romao.images/images/ManuelRomao.jpg")
# MAGIC <!-- display("dbfs:/mnt/tese.manuel.romao.images/images/ManuelRomao.jpg") -->
