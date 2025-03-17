import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Function to delete duplicated files 
def delete_s3_files(bucket, prefix):
    objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in objects_to_delete:
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": obj["Key"]} for obj in objects_to_delete["Contents"]]}
        )

s3_path = "s3://acoe-silver-layer/test_intern/kbz-pay-transactions/KBZPay_ActionsInTransactions_22022025.csv"

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s3_path)
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

from pyspark.sql.functions import col, to_timestamp
import re 

filename = s3_path.split("/")[-1].split(".")[0]
date = filename.split("_")[-1]
date_obj = datetime.strptime(date, '%d%m%Y')
prev_day = date_obj - timedelta(days=1)

day_key = prev_day.strftime('%Y%m%d') 
month_key = prev_day.strftime('%Y%m') 
year_key = prev_day.strftime('%Y')

dest = "s3://acoe-silver-layer/test_intern/kbz-pay-parquet/"

# ApplyMapping transformation to rename columns and change their data types

column_mappings = [
    ("Transaction ID", "transaction_id"),
    ("Status", "status"),
    ("Initiation time", "initiation_time"),
    ("Last update time", "last_update_time"),
    ("Transaction type", "transaction_type"),
    ("Reason type", "reason_type"),
    ("Initiator", "initiator"),
    ("Initiator type", "initiator_type"),
    ("Requester", "requester"),
    ("Requester type", "requester_type"),
    ("Channel", "channel"),
    ("Amount", "amount"),
    ("Debit Party Charge", "debit_party_charge"),
    ("Debit Party Commission", "debit_party_commission"),
    ("Credit Party Charge", "credit_party_charge"),
    ("Credit Party Commission", "credit_party_commission"),
    ("Failure description", "failure_description"),
    ("Debit Party", "debit_party"),
    ("Debit Party Type", "debit_party_type"),
    ("Credit Party", "credit_party"),
    ("Credit Party Type", "credit_party_type"),
    ("Account entries", "account_entries"),
    ("Ref Data", "ref_data"),
    ("Link transaction ID", "link_transaction_id"),
    ("Approver", "approver"),
    ("Approver type", "approver_type"),
    ("Approver time", "approver_time"),
    ("ConversationID", "conversation_id"),
    ("OriginatorConversationID", "originator_conversation_id"),
    ("Credit_Customer_ID", "credit_customer_id"),
    ("Debit_Customer_ID", "debit_customer_id"),
    ("Cost(Principle Commission)", "cost_principle_commission"),
    ("Cost(Residual Commission)", "cost_residual_commission"),
    ("Revenue(Service Fee)", "revenue_service_fee"),
    ("SettlementService Type", "settlement_service_type"),
    ("3rd_party_charge", "third_party_charge"),
    ("original_order_id", "original_order_id"),
]



transformed_df = df.select(
    *[
        col(source).cast("string").alias(alias)
        for source, alias in column_mappings
    ]
)


df = transformed_df.toDF()
df = df.withColumn("initiation_time", to_timestamp(col("initiation_time").cast("string"), "yyyyMMddHHmmss"))
df = df.withColumn("last_update_time", to_timestamp(col("last_update_time").cast("string"), "yyyyMMddHHmmss"))

df = df \
            .withColumn("file_name", lit(filename))\
            .withColumn("year_key",lit(year_key))\
            .withColumn("month_key", lit(month_key))\
            .withColumn("day_key", lit(day_key))
            
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

delete_s3_files(s3_bucket, s3_prefix)

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": dest,
        "partitionKeys": ["year_key","month_key","day_key"],  
        "catalogDatabase": "database_test",
        "catalogTableName": "split-files",
        "enableUpdateCatalog": True
    },
    format="parquet"
)

job.commit()