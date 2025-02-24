import sys
import boto3
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

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1740121850329 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, 
    connection_type="s3", 
    format="csv",
    connection_options={"paths": ["s3://acoe-silver-layer/KBZpay_market/source/Category_of_KBZPay_Market_20230617.csv"], "recurse": True}, 
    transformation_ctx="AmazonS3_node1740121850329")

file = "Category_of_KBZPay_Market_20230617.csv"
date = file.split("_")[-1].split(".")[0]
date_obj = datetime.strptime(date, '%Y%m%d')
prev_day = date_obj - timedelta(days=1)

day_key = prev_day.strftime('%Y%m%d') 
month_key = prev_day.strftime('%Y%m') 
year_key = prev_day.strftime('%Y')

AmazonS3_node1740121850329 = AmazonS3_node1740121850329.toDF()

AmazonS3_node1740121850329 = AmazonS3_node1740121850329.withColumn("file_name", lit(file))
AmazonS3_node1740121850329 = AmazonS3_node1740121850329 \
                                .withColumn("year_key", lit(year_key))\
                                .withColumn("month_key", lit(month_key))\
                                .withColumn("day_key", lit(day_key))


AmazonS3_node1740121850329 = DynamicFrame.fromDF(AmazonS3_node1740121850329, glueContext, "AmazonS3_node1740121850329")

s3_bucket = "acoe-silver-layer"
s3_prefix = f"KBZpay_market/processed/year_key={year_key}/month_key={month_key}/day_key={day_key}/"
s3_client = boto3.client("s3")

def delete_s3_files(bucket, prefix):
    objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in objects_to_delete:
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": obj["Key"]} for obj in objects_to_delete["Contents"]]}
        )

# Script generated for node Change Schema
ChangeSchema_node1740122549634 = ApplyMapping.apply(
    frame=AmazonS3_node1740121850329, 
    mappings=[("level 1 category id", "string", "level_1_category_id", "string"), 
              ("level 1 category name", "string", "level_1_category_name", "string"), 
              ("level 2 category id", "string", "level_2_category_id", "string"), 
              ("level 2 category name", "string", "level_2_category_name", "string"), 
              ("level 3 category id", "string", "level_3_category_id", "string"), 
              ("level 3 category name", "string", "level_3_category_name", "string"), 
              ("created user id", "string", "created_user_id", "string"), 
              ("created date", "string", "created_date", "string"), 
              ("status", "string", "status", "string"), 
              ("updated user id", "string", "updated_user_id", "string"), 
              ("last updated date", "string", "last_updated_date", "string"),
              ("file_name", "string","file_name", "string"),
              ("year_key", "string", "year_key", "string"),
              ("month_key", "string", "month_key", "string"),
              ("day_key", "string", "day_key", "string"),], 
              transformation_ctx="ChangeSchema_node1740122549634")

# Script generated for node SQL Query
SqlQuery0 = '''
select level_1_category_id, 
level_1_category_name,
level_2_category_id,
level_2_category_name, 
level_3_category_id, 
level_3_category_name, 
created_user_id, 
CAST(created_date AS TIMESTAMP) AS created_date,
status, 
updated_user_id, 
CAST(last_updated_date AS TIMESTAMP) AS last_updated_date,
file_name, 
year_key,
month_key,
day_key
from myDataSource

'''
SQLQuery_node1740122684021 = sparkSqlQuery(
    glueContext, query = SqlQuery0, 
    mapping = {"myDataSource":ChangeSchema_node1740122549634}, 
    transformation_ctx = "SQLQuery_node1740122684021")

delete_s3_files(s3_bucket, s3_prefix)

AWSGlueDataCatalog_node1740122845129 = glueContext.getSink(
                            path="s3://acoe-silver-layer/KBZpay_market/processed/", 
                            connection_type="s3",
                            updateBehavior="UPDATE_IN_DATABASE", 
                            partitionKeys=["year_key","month_key","day_key"], 
                            enableUpdateCatalog=True, 
                            transformation_ctx="AWSGlueDataCatalog_node1740122845129"
                            )

AWSGlueDataCatalog_node1740122845129.setCatalogInfo(
    catalogDatabase="database_test",
    catalogTableName="kbzpay_market"
    )

AWSGlueDataCatalog_node1740122845129.setFormat("glueparquet", compression="snappy")
AWSGlueDataCatalog_node1740122845129.writeFrame(SQLQuery_node1740122684021)
job.commit()