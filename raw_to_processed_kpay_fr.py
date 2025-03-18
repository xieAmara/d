import sys
import boto3
import pytz
import pandas as pd
import re
import openpyxl
from datetime import datetime, timedelta
from pytz import timezone
from pyspark.sql import functions as F
from io import BytesIO
from awsglue.dynamicframe import DynamicFrame 
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import col, to_date
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import year, month, lit, when, regexp_replace

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_client=boto3.client('s3')
bucket = 'acoe-silver-layer'
prefix = 'kpay_fr/source/'

response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

# s3://acoe-silver-layer/kpay_fr/source/year_key=2024/month_key=202408/day_key=20240812/kpay_fr_20240812.xlsx
files = [] 
date_keys = []

if 'Contents' in response:
    for item in response['Contents']:
        file_name = item['Key'].split("/")[-1]
        if file_name.startswith("kpay_fr") and file_name.endswith(".xlsx"): 
            match = re.match(r'kpay_fr_(\d{4})(\d{2})(\d{2})\.xlsx', file_name)
            if match:
                year, month, day = match.groups()
                date_keys.append((year, month, day, item['Key']))
                files.append(file_name) 

print("Extracted Files:", files)
print("Date Keys:", date_keys)
def delete_s3_files(bucket, prefix):
    objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in objects_to_delete:
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": obj["Key"]} for obj in objects_to_delete["Contents"]]}
        )

expected_columns = [
    "case_no", 
    "fraud_incident_from_date", 
    "fraud_incident_to_date", 
    "fraud_discover_date", 
    "fmu_fraud_discover_date",
    "description",
    "how_detected",
    "ticket_number", 
    "investigation_team", 
    "fraud_category", 
    "fraud_type",
    "initial_loss_amount_mmk", 
    "recovery_amount_mmk", 
    "net_loss_amount_mmk", 
    "trends", 
    "fraudster_group", 
    "status",
    "status_update", 
    "latest_update", 
    "assigned_to_followup", 
    "impact_phone",
    "first_related_phone",
    "second_related_phone", 
    "third_related_phone", 
    "fourth_related_phone", 
    "fifth_related_phone",
    "sixth_related_phone", 
    "seventh_related_phone", 
    "eighth_related_phone", 
    "fraud_pattern_name", 
    "remark",
    "year_key",
    "month_key",
    "day_key"
]

for year, month, day, file_path in date_keys:
    bucket = 'acoe-silver-layer'
    key = file_path
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)

    df_pd = pd.read_excel(BytesIO(obj["Body"].read()), engine="openpyxl")

    # Create schema where all columns are StringType except the specified date columns
    schema = StructType([
        StructField(col, StringType(), True) if col not in [
            "fraud_incident_from_date", 
            "fraud_incident_to_date", 
            "fraud_discover_date", 
            "fmu_fraud_discover_date"
        ] else StructField(col, DateType(), True) for col in df_pd.columns  # Using the DataFrame's columns
    ])

    # Convert pandas DataFrame to Spark DataFrame with the defined schema
    df = spark.createDataFrame(df_pd, schema=schema)
    
    date = f"{year}{month}{day}"
    date_obj = datetime.strptime(date, '%Y%m%d')
    prev_day = date_obj - timedelta(days=1)

    day_key = prev_day.strftime('%Y%m%d') 
    month_key = prev_day.strftime('%Y%m') 
    year_key = prev_day.strftime('%Y')
    
    df = df \
    .withColumn("year_key",lit(year_key))\
    .withColumn("month_key", lit(month_key))\
    .withColumn("day_key", lit(day_key))
    
    cleaned_columns = [
    c.replace("\n", "").replace("\r", "").strip().lower().replace(' ', '').replace('_ ', '_').replace(' _', '_')
    for c in df.columns
    ]
    df = df.toDF(*cleaned_columns)
    
    df = df \
        .withColumn("fraud_incident_from_date", to_date(col("fraud_incident_from_date"), "yyyy/MM/dd")) \
        .withColumn("fraud_incident_to_date", to_date(col("fraud_incident_to_date"), "yyyy/MM/dd")) \
        .withColumn("fraud_discover_date", to_date(col("fraud_discover_date"), "yyyy/MM/dd")) \
        .withColumn("fmu_fraud_discover_date", to_date(col("fmu_fraud_discover_date"), "yyyy/MM/dd"))
    
    df.show(10)
    
    all_columns = df.columns 
    if sorted(all_columns) == sorted(expected_columns) and len(all_columns) == len(expected_columns):
        df = df.withColumn(
            "impact_phone", 
            F.regexp_replace("impact_phone", '"', '')  
        )
        
        df = df.withColumn(
            "impact_phone", 
            F.when(
                ~F.isnan("impact_phone"), 
                F.concat(F.lit('{"impact_phone_json":["'), F.col("impact_phone"), F.lit('"]}'))
            ).otherwise(None)  
        )
        
        df = df.withColumn(
            "first_related_phone", 
            F.when(
                ~F.isnan("first_related_phone"),
                F.concat(F.lit('{"first_related_phone_json":['), F.col("first_related_phone"), F.lit(']}'))
            ).otherwise(None)
        )
        
        df = df.withColumn(
            "second_related_phone", 
            F.when(
                ~F.isnan("second_related_phone"),
                F.concat(F.lit('{"second_related_phone_json":['), F.col("second_related_phone"), F.lit(']}'))
            ).otherwise(None)
        )
        
        df = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
        
        s3_bucket = "acoe-silver-layer"
        s3_prefix = f"kpay_fr/processed/year_key={year_key}/month_key={month_key}/day_key={day_key}/"
        delete_s3_files(s3_bucket, s3_prefix)

        AmazonS3_node1730101467610 = glueContext.getSink(
            path="s3://acoe-silver-layer/kpay_fr/processed/", 
            connection_type="s3",
            updateBehavior="UPDATE_IN_DATABASE", 
            partitionKeys=["year_key", "month_key", "day_key"], 
            enableUpdateCatalog=True, 
            transformation_ctx="AmazonS3_node1730101467610")
        
        AmazonS3_node1730101467610.setCatalogInfo(catalogDatabase="database_test", catalogTableName="kpay_fraud")
        AmazonS3_node1730101467610.setFormat("glueparquet", compression="snappy")
        AmazonS3_node1730101467610.writeFrame(df)

    else:
        print("columns does not match")
        break
    
job.commit()