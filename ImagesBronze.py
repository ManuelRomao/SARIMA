# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *

# url processing
import urllib

# COMMAND ----------

df = spark.read.format("image").load("/mnt/tese.manuel.romao.images/images/")
display(df)

# COMMAND ----------

sample_img_dir = "/mnt/tese.manuel.romao.images/images/"

# COMMAND ----------

sample_img_dir

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC dbutils.fs/Tables.put("dbfs:/mnt/tese.manuel.romao.images/images/ManuelRomao.jpg", "This is the actual text that will be saved to disk. Like a 'Hello world!' example")
# MAGIC FileStore/tables

# COMMAND ----------

image_df = spark.read.format("image").load("dbfs:/mnt/tese.manuel.romao.images/images/ManuelRomao.jpg")

display(image_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ![GrabNGoInfo Logo](files/images/ManuelRomao.jpg)

# COMMAND ----------

# MAGIC %fs ls FileStore/tables

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls FileStore/images

# COMMAND ----------

display(image_df.select("image.origin"))

# COMMAND ----------

displayHTML("<img src ='dbfs:/mnt/tese.manuel.romao.images/images/ManuelRomao.jpg'>")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ![my_test_image]("dbfs:/mnt/tese.manuel.romao.images/images/ManuelRomao.jpg")
# MAGIC <!-- display("dbfs:/mnt/tese.manuel.romao.images/images/ManuelRomao.jpg") -->
