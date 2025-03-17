
import sys
import boto3
import pytz
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta
from pyspark.sql.functions import year, month, lit, when, regexp_replace
from pytz import timezone
from awsglue.dynamicframe import DynamicFrame 
from pyspark.sql.functions import col

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database='database_test', 
    table_name='split-files'
)

# Convert AWS Glue DynamicFrame to Spark DataFrame
df_spark = dynamic_frame.toDF()
# Define export path in S3
export_path = "s3://acoe-silver-layer/test_intern/kbz-pay-transactions/"

# Export to s3 
df_spark.coalesce(1).write.mode("overwrite").option("header", "true").csv(export_path)
s3 = boto3.client("s3")

bucket_name = "acoe-silver-layer"
source_folder = "test_intern/kbz-pay-transactions/"
destination_file = "test_intern/kbz-pay-transactions/final_kpay_transaction.csv"

# List objects 
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_folder)

# Find the file
csv_file_key = None
for obj in response.get("Contents", []):
    if obj["Key"].endswith(".csv"): 
        csv_file_key = obj["Key"]
        break

if csv_file_key:    
    temp_file = "/tmp/temp_large_file.csv"
    s3.download_file(bucket_name, csv_file_key, temp_file)
    s3.upload_file(temp_file, bucket_name, destination_file)
    s3.delete_object(Bucket=bucket_name, Key=csv_file_key)

job.commit()