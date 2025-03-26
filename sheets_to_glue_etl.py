import sys
import requests
import json
import pandas as pd
import boto3 
import re 
import pytz
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame 
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta
from pyspark.sql.functions import year, month, lit, when, regexp_replace, col
from pytz import timezone

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define functions 
def delete_s3_files(bucket, prefix):
    objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in objects_to_delete:
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": obj["Key"]} for obj in objects_to_delete["Contents"]]}
        )

SHEET_ID = "1tXXdkYzW73-QWH_Uz14ZfVhdpcM404DWtR7rcYTtr1A"
API_KEY = "AIzaSyCXfkMf0thA_iVjWQf5AsbJGsuN5CT4aC4"
SHEET_NAME = "KYC Officer" 

# Construct Google Sheets API URL
url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{SHEET_NAME}?key={API_KEY}"

# Fetch Data as a json file 
response = requests.get(url)
data = response.json()

# Convert data to Pandas DataFrame
columns = data["values"][1]  
rows = data["values"][2:]   
pdf = pd.DataFrame(rows, columns=columns)

# change from pandas to spark 
df = spark.createDataFrame(pdf)

# Create data date column 
date_obj = datetime.now(timezone('Asia/Yangon'))
data_date = date_obj.strftime('%Y%m%d')

df = df.withColumn("data_date",lit(data_date))

# Clean column name 
cleaned_columns = [
c.replace("\n", "").replace("\r", "").strip().lower().replace(' ', '_').replace('_ ', '_').replace(' _', '_')
for c in df.columns
]
df = df.toDF(*cleaned_columns)

dyn_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

s3_client = boto3.client("s3")
s3_bucket = "acoe-silver-layer"
s3_prefix = "test_intern/google_sheets_processed/"
delete_s3_files(s3_bucket, s3_prefix)

amazons3 = glueContext.getSink(
    path="s3://acoe-silver-layer/test_intern/google_sheets_processed/", 
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE", 
    enableUpdateCatalog=True, 
    transformation_ctx="amazons3"
)

amazons3.setCatalogInfo(catalogDatabase="database_test", catalogTableName="google_sheets_kycOfficer")
amazons3.setFormat("glueparquet", compression="snappy")
amazons3.writeFrame(dyn_frame)

job.commit()