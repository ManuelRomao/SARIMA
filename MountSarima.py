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
# AWS_S3_BUCKET_CHEKPOINT = 'tese.manuel.romao/checkPoints'
AWS_S3_BUCKET_GOLD_CHEKPOINT = 'tese.manuel.romao/checkPointGold'
# AWS_S3_BUCKET_DATA = 'tese.manuel.romao/data'

# mount name for the bucket
# MOUNT_NAME_CHECKPOINT = "/mnt/tese.manuel.romao/checkPoints"
MOUNT_NAME_GOLD_CHECKPOINT = "/mnt/tese.manuel.romao/checkPointGold"
# MOUNT_NAME_DATA = "/mnt/tese.manuel.romao/data"

# source url
# SOURCE_URL_CHECKPOINT = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCONDED_SECRET_KEY, AWS_S3_BUCKET_CHEKPOINT)
SOURCE_URL_GOLD_CHECKPOINT = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCONDED_SECRET_KEY, AWS_S3_BUCKET_GOLD_CHEKPOINT)
# SOURCE_URL_DATA = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCONDED_SECRET_KEY, AWS_S3_BUCKET_DATA)

#mount the drive 
# dbutils.fs.mount(SOURCE_URL_CHECKPOINT, MOUNT_NAME_CHECKPOINT)
dbutils.fs.mount(SOURCE_URL_CHECKPOINT, MOUNT_NAME_GOLD_CHECKPOINT)
# dbutils.fs.mount(SOURCE_URL_DATA, MOUNT_NAME_DATA)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# dbutils.fs.unmount("/mnt/tese.manuel.romao/data")
dbutils.fs.unmount("/mnt/tese.manuel.romao/checkPoints")

# COMMAND ----------

# dbutils.fs.rm("/mnt/tese.manuel.romao/data",True)
dbutils.fs.rm("/mnt/tese.manuel.romao/checkPoints",True)

# COMMAND ----------

display(dbutils.fs.mounts())
