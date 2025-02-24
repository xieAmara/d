
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
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# identify path 

source = "s3://acoe-silver-layer/kpay_customer_detail/"
dest = "s3://acoe-silver-layer/test_intern/kpay_cust_detail_processed/"

# retrieve current date 

current_time = datetime.now(timezone('Asia/Yangon'))
day_key = (current_time - timedelta(days=29)).strftime("%d%m%Y")
month_key = (current_time - timedelta(days=29)).strftime("%m%Y")
year_key = (current_time - timedelta(days=29)).strftime("%Y")

# retrieve file 
file = source + "KBZPay_CustomerDetail_" + day_key + ".csv"
custom_options = {
    "header": True,
    "inferSchema": False,
}
df = spark.read.options(**custom_options).csv(file)
# insert partition keys 
df = df \
    .withColumn("year_key",lit(year_key))\
    .withColumn("month_key", lit(month_key))\
    .withColumn("day_key", lit(day_key))

df.printSchema()
# change column name 
def change_column_name(df):
    new_names = []
    for name in df.columns:
        if not re.match(r"^[a-zA-Z0-9_]+$", name): 
            new_name = re.sub(r"[^a-zA-Z0-9_]", "_", name).lower().replace(" ", "_")
        else:
            new_name = name.lower().replace(" ", "_") 
        new_names.append(new_name)

    for i, old_name in enumerate(df.columns):
        df = df.withColumnRenamed(old_name, new_names[i])
    return df

df_changed_schema = change_column_name(df)
df_changed_schema.show()
# data type transformation 
dynamic_frame = DynamicFrame.fromDF(df_changed_schema, glueContext)

changecolumnname_node = ApplyMapping.apply(
    frame=dynamic_frame,
    mappings=[
        ("phone_no", "string", "phone_no", "string"),
        ("customer_name", "string", "customer_name", "string"),
        ("customer_nrc_passport", "string", "customer_nrc_passport", "string"),
        ("trust_level", "string", "level", "string"),
        ("status", "string", "status", "string"),
        ("date_1st_login_for_one_month", "string", "date_1st_login_for_one_month", "string"),
        ("no__of_login_for_one_month", "string", "no_of_login_for_one_month", "string"),
        ("date_registered", "string", "date_registered", "string"),
        ("date_1st_login", "string", "date_1st_login", "string"),
        ("no__of_login", "string", "no_of_login", "string"),
        ("customer_id", "decimal", "customer_id", "string"),
        ("date_of_birth", "string", "date_of_birth", "string"),
        ("permanent_address", "string", "permanent_address", "string"),
        ("township", "string", "township", "string"),
        ("state_division", "string", "state_division", "string"),
        ("nationality", "string", "nationality", "string"),
        ("gender", "string", "gender", "string"),
        ("town", "string", "town", "string"),
        ("modifiedon", "string", "modifiedon", "string"),
        ("casa_flag", "string", "casa_flag", "string"),
        ("email_address", "string", "email_address", "string"),
        ("primary_id_type", "string", "primary_id_type", "string"),
        ("secondary_id_type", "string", "secondary_id_type", "string"),
        ("sub_trust_level", "string", "sub_trust_level", "string"),
        ("employment_type", "string", "employment_type", "string"),
        ("segment", "string", "segment", "string"),
        ("customer_trust_level_detail", "string", "customer_trust_level_detail", "string"),
        ("year_key", "string", "year_key", "string"),
        ("month_key", "string", "month_key", "string"),
        ("day_key", "string", "day_key", "string"),
    ],
    transformation_ctx="changecolumnname_node",
)

from pyspark.sql.functions import to_timestamp

df_change = changecolumnname_node.toDF()
df_change = df_change.withColumn("date_registered", to_timestamp("date_registered", "yyyy/MM/dd HH:mm:ss"))
df_change.printSchema()
# Make sure there aren't duplicates 
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

delete_s3_files(s3_bucket, s3_prefix)
dynamic_frame = DynamicFrame.fromDF(df_change, glueContext, "dynamic_frame") 

s3_path_input = glueContext.getSink(
                            path="s3://acoe-silver-layer/test_intern/kpay_cust_detail_processed/", 
                            connection_type="s3",
                            updateBehavior="UPDATE_IN_DATABASE", 
                            partitionKeys=["year_key","month_key","day_key"], 
                            enableUpdateCatalog=True, 
                            transformation_ctx="s3_path_input"
                            )

s3_path_input.setCatalogInfo(
    catalogDatabase="database_test",
    catalogTableName="kpay_cust_table"
    )

s3_path_input.setFormat("glueparquet", compression="snappy")
s3_path_input.writeFrame(dynamic_frame)
job.commit()