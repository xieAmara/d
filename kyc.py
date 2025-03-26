import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import datetime, timedelta
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

current_datetime = datetime.now(timezone('Asia/Yangon'))
read_year_key = current_datetime.year
read_month_key = int(current_datetime.strftime("%Y%m"))
read_day_key = int(current_datetime.strftime("%Y%m%d"))

# Script generated for node Amazon S3
AmazonS3_node1740038144265 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3", format="json",
    connection_options={"paths": 
        [f"s3://acoe-datalake-raw/de-staging/kpay_kyc_officer_list/year={read_year_key}/month={read_month_key}/day={read_day_key}/kpay_kyc_officer_list_{read_day_key}.json"],
        "recurse": True},
    transformation_ctx="AmazonS3_node1740038144265")

# Script generated for node Change Schema
ChangeSchema_node1740038243153 = ApplyMapping.apply(
    frame=AmazonS3_node1740038144265,
    mappings=
    [("Employee Id", "string", "employee_id", "string"),
    ("Name", "string", "name", "string"),
    ("Email Address", "string", "email_address", "string"),
    ("Department", "string", "department", "string"),
    ("User Group", "string", "user_group", "string"),
    ("User Status", "string", "user_status", "string")],
    transformation_ctx="ChangeSchema_node1740038243153")

# Script generated for node SQL Query
SqlQuery1214 = '''
select 
TRIM(employee_id) AS employee_id,
TRIM(name) AS name,
TRIM(email_address) AS email_address,
TRIM(department) AS department,
TRIM(user_group) AS user_group,
TRIM(user_status) AS user_status,
to_date(from_utc_timestamp(current_timestamp(), 'Asia/Yangon'),"yyyy-mm-dd") AS data_date
FROM myDataSource
'''
SQLQuery_node1740038389136 = sparkSqlQuery(glueContext, query = SqlQuery1214, mapping = {"myDataSource":ChangeSchema_node1740038243153}, transformation_ctx = "SQLQuery_node1740038389136")

# Script generated for node Amazon Redshift
AmazonRedshift_node1740038487480 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1740038389136, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-390295393321-ap-southeast-1/temporary/", "useConnectionProperties": "true", "dbtable": "lookup.kpay_operation_officer_list", "connectionName": "redsift_kbzanalytics", "preactions": "CREATE TABLE IF NOT EXISTS lookup.kpay_operation_officer_list (employee_id VARCHAR, name VARCHAR, email_address VARCHAR, department VARCHAR, user_group VARCHAR, user_status VARCHAR, data_date DATE); TRUNCATE TABLE lookup.kpay_operation_officer_list;"}, transformation_ctx="AmazonRedshift_node1740038487480")

job.commit()