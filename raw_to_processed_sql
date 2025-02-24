import sys
import boto3
import pytz
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import datetime, timedelta
from pyspark.sql.functions import lit, year, month
from pytz import timezone 

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

# identify path 

source = "s3://acoe-silver-layer/kpay_customer_detail/"
dest = "s3://acoe-silver-layer/test_intern/kpay_cust_detail_processed/"

# retrieve current date 

current_time = datetime.now(timezone('Asia/Yangon'))
day_key = (current_time - timedelta(days=27)).strftime("%d%m%Y")
month_key = (current_time - timedelta(days=27)).strftime("%m%Y")
year_key = (current_time - timedelta(days=27)).strftime("%Y")


# retrieve file 
file = source + "KBZPay_CustomerDetail_" + day_key + ".csv"
custom_options = {
    "header": True,
    "inferSchema": False,
}

# Script generated for node Amazon S3
AmazonS3_node1739262298731 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, 
    connection_type="s3", 
    format="csv", 
    connection_options={"paths": [source + "KBZPay_CustomerDetail_" + day_key + ".csv"]}, 
    transformation_ctx="AmazonS3_node1739262298731")

AmazonS3_node1739262298731 = AmazonS3_node1739262298731.toDF()

AmazonS3_node1739262298731 = AmazonS3_node1739262298731 \
    .withColumn("year_key",lit(year_key))\
    .withColumn("month_key", lit(month_key))\
    .withColumn("day_key", lit(day_key))

AmazonS3_node1739262298731 = DynamicFrame.fromDF(AmazonS3_node1739262298731, glueContext, "AmazonS3_node1739262298731")

s3_bucket = "acoe-silver-layer"
s3_prefix = f"test_intern/kpay_cust_detail_processed/year_key={year_key}/month_key={month_key}/day_key={day_key}/"
s3_client = boto3.client("s3")


def delete_s3_files(bucket, prefix):
    objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in objects_to_delete:
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": obj["Key"]} for obj in objects_to_delete["Contents"]]}
        )

# Script generated for node Change Schema
ChangeSchema_node1739262502975 = ApplyMapping.apply(
    frame=AmazonS3_node1739262298731, 
    mappings=[("phone no", "string", "phone_no", "string"), 
    ("customer name", "string", "customer_name", "string"), 
    ("customer nrc/passport", "string", "customer_nrc_passport", "string"), 
    ("level", "string", "level", "string"), 
    ("status", "string", "status", "string"), 
    ("date 1st login for one month", "string", "date_1st_login_for_one_month", "string"), 
    ("`no. of login for one month`", "string", "no_of_login_for_one_month", "string"), 
    ("date registered", "string", "date_registered", "timestamp"), 
    ("date 1st login", "string", "date_1st_login", "string"), 
    ("`no. of login`", "string", "no_of_login", "string"), 
    ("customer_id", "string", "customer_id", "string"), 
    ("date of birth", "string", "date_of_birth", "string"), 
    ("permanent address", "string", "permanent_address", "string"), 
    ("township", "string", "township", "string"), 
    ("state/division", "string", "state_division", "string"), 
    ("nationality", "string", "nationality", "string"), 
    ("gender", "string", "gender", "string"), 
    ("town", "string", "town", "string"), 
    ("modifiedon", "string", "modifiedon", "string"), 
    ("casa flag", "string", "casa_flag", "string"), 
    ("email address", "string", "email_address", "string"), 
    ("primary id type", "string", "primary_id_type", "string"), 
    ("secondary id type", "string", "secondary_id_type", "string"), 
    ("sub trust level", "string", "sub_trust_level", "string"), 
    ("employment type", "string", "employment_type", "string"), 
    ("segment", "string", "segment", "string"), 
    ("customer trust level detail", "string", "customer_trust_level_detail", "string"),
    ("year_key", "string", "year_key", "string"),
    ("month_key", "string", "month_key", "string"),
    ("day_key", "string", "day_key", "string"),], 
    transformation_ctx="ChangeSchema_node1739262502975")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT phone_no,
       customer_name,
       customer_nrc_passport,
       level,
       status,
       date_1st_login_for_one_month,
       no_of_login_for_one_month,
       TO_TIMESTAMP(date_registered, 'yyyy/MM/dd HH:mm:ss') AS date_registered,
       date_1st_login,
       no_of_login,
       customer_id,
       date_of_birth,   
       permanent_address,
       township,
       state_division,
       nationality,
       gender,
       town,
       modifiedon,
       casa_flag,
       email_address,
       primary_id_type,
       secondary_id_type,
       sub_trust_level,
       employment_type,
       segment,
       customer_trust_level_detail, 
       year_key,
       month_key,
       day_key  
from myDataSource

'''
SQLQuery_node1739262701088 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":ChangeSchema_node1739262502975}, transformation_ctx = "SQLQuery_node1739262701088")

delete_s3_files(s3_bucket, s3_prefix)

# Script generated for node Amazon S3

AmazonS3_node1739262936552 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1739262701088,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://acoe-silver-layer/test_intern/kpay_cust_detail_processed/",
        "partitionKeys": ["year_key", "month_key", "day_key"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1739262936552",
)

# AmazonS3_node1739262936552.setCatalogInfo(
#     catalogDatabase="database_test",
#     catalogTableName="kpay_cust_detail_table")

job.commit()