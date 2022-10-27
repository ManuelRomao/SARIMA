# Databricks notebook source
display(dbutils.fs.ls("/FileStore/tables"))

# COMMAND ----------

# pyspark functions
from pyspark.sql.functions import *

# url processing
import urllib

# COMMAND ----------

#Define file type 
file_type = "csv"
first_row_is_header = "true"
delimiter = ","

#Read csv file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
    .option("header", first_row_is_header)\
    .option("sep", delimiter)\
    .load("/FileStore/tables/new_user_credentials.csv")

# COMMAND ----------

#Get AWS access key and secret key from the spark Dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='Databricks-Thesis').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='Databricks-Thesis').select('Secret access key').collect()[0]['Secret access key']

#Encode the secret key
ENCONDED_SECRET_KEY = urllib.parse.quote(string = SECRET_KEY, safe = "")

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET_IMAGES = 'tese.manuel.romao.images/images'

# mount name for the bucket
MOUNT_NAME_IMAGES = "/mnt/tese.manuel.romao.images/images"

# source url
SOURCE_URL_CHECKPOINT_IMAGES = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCONDED_SECRET_KEY, AWS_S3_BUCKET_IMAGES)

# mount the drive 
dbutils.fs.mount(SOURCE_URL_CHECKPOINT_IMAGES, MOUNT_NAME_IMAGES)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/tese.manuel.romao.images/images")
dbutils.fs.unmount("/mnt/tese.manuel.romao.images/checkPoints")

# COMMAND ----------

# dbutils.fs.unmount("/mnt/tese.manuel.romao/data")
# dbutils.fs.unmount("/mnt/tese.manuel.romao/checkPoints")

# COMMAND ----------

# dbutils.fs.rm("/mnt/tese.manuel.romao/data",True)
# dbutils.fs.rm("/mnt/tese.manuel.romao/checkPoints",True)

# COMMAND ----------

display(dbutils.fs.mounts())
