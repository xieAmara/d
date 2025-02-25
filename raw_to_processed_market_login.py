import sys
import boto3
import pytz
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta
from pyspark.sql.functions import year, month, lit
from pytz import timezone
from awsglue.dynamicframe import DynamicFrame 
from pyspark.sql.functions import col

# Define functions 
def delete_s3_files(bucket, prefix):
    objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in objects_to_delete:
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": obj["Key"]} for obj in objects_to_delete["Contents"]]}
        )

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Loop Folder 
s3 = boto3.client('s3')
source = "s3://acoe-silver-layer/market_customerlogin/source/"
dest = "s3://acoe-silver-layer/market_customerlogin/processed/"

bucket_name = "acoe-silver-layer"
prefix = "market_customerlogin/"

response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

if 'Contents' in response:
    for obj in response['Contents']:
        key = obj['Key']
        filename = key.replace(prefix, '') 

        if filename.endswith('.csv'):
            s3_path = f"s3://{bucket_name}/{key}"
            custom_options = {
                "header": True,
                "inferSchema": False,
            }

            # Use a different way to retrieve the day key 
            df = spark.read.options(**custom_options).csv(s3_path)
            filename = s3_path.split("/")[-1].split(".")[0]
            date = filename[-8:]
            date_obj = datetime.strptime(date, '%d%m%Y')
            prev_day = date_obj - timedelta(days=1)

            day_key = prev_day.strftime('%Y%m%d') 
            month_key = prev_day.strftime('%Y%m') 
            year_key = prev_day.strftime('%Y')

            df = df \
            .withColumn("file_name", lit(filename))\
            .withColumn("year_key",lit(year_key))\
            .withColumn("month_key", lit(month_key))\
            .withColumn("day_key", lit(day_key))

            # Set default values if the columns are not present 

            if "source" not in df.columns: 
                df = df.withColumn("source", lit("Mobile Phone"))
            if "role" not in df.columns:
                df = df.withColumn("role", lit("Customer"))

            df = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
            s3_bucket = "acoe-silver-layer"
            s3_prefix = f"market_customerlogin/processed/year_key={year_key}/month_key={month_key}/day_key={day_key}/"
            
            changecolumnname_node = ApplyMapping.apply(
                frame=df,
                mappings=[
                    ("customer name", "string", "customer_name", "string"), 
                    ("phone number", "string", "phone_number", "string"), 
                    ("customer id", "string", "customer_id", "string")
                    ("source", "string", "source", "string"),
                    ("role", "string", "role", "string"),  
                    ("file_name", "string","file_name", "string"),
                    ("year_key", "string", "year_key", "string"),
                    ("month_key", "string", "month_key", "string"),
                    ("day_key", "string", "day_key", "string"),
                ], 
                transformation_ctx="changecolumnname_node",
            )

            delete_s3_files(s3_bucket, s3_prefix)

            AWSGlueDataCatalog_node1740454085980 = glueContext.getSink(
                                        path="s3://acoe-silver-layer/market_customerlogin/processed/", 
                                        connection_type="s3",
                                        updateBehavior="UPDATE_IN_DATABASE", 
                                        partitionKeys=["year_key","month_key","day_key"], 
                                        enableUpdateCatalog=True, 
                                        transformation_ctx="AWSGlueDataCatalog_node1740454085980"
                                        )

            AWSGlueDataCatalog_node1740454085980.setCatalogInfo(
                catalogDatabase="database_test",
                catalogTableName="market_login"
                )

            AWSGlueDataCatalog_node1740454085980.setFormat("glueparquet", compression="snappy")
            AWSGlueDataCatalog_node1740454085980.writeFrame(changecolumnname_node)   

job.commit()