import sys
import re
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import trim, col, regexp_replace, lit

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = glueContext.spark_session.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
    

def validate_columns(df, expected_columns):
    actual_columns = sorted([col.lower() for col in df.columns])
    expected_columns = sorted([col.lower() for col in expected_columns])
    if actual_columns != expected_columns:
        raise ValueError(f"Column mismatch. Expected: {expected_columns}, Found: {actual_columns}")

# Function to change column names if they are only spaces
def change_col_name(df):
    return df.select([
        col(c).alias('center_code') if c.strip() == '' or c.strip() == ' ' else col(c)
        for c in df.columns
    ])

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 Client and Path Variables
s3_client = boto3.client('s3')
bucket = 'acoe-datalake-raw'
prefix = 'sftp_mount_10-11-40-51/KBZPay/KBZPayCenter/'

# List files from S3
response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

# Filter files that match the expected pattern
filtered_files = []
date_values = []

if 'Contents' in response:
    for item in response['Contents']:
        file_name = item['Key'].split('/')[-1]
        if "KBZPay_Center_Agent Mapping_Update_2024-01-15.csv" in file_name:
            continue
        if file_name.startswith("KBZPay_Center_Agent Mapping_Update_") and file_name.endswith(".csv"):
            # Extract the date from the filename
            match = re.match(r'KBZPay_Center_Agent Mapping_Update_(\d{4})-(\d{2})-(\d{2})\.csv', file_name)
            if match:
                year, month, day = match.groups()
                date_value = f"{year}-{month}-{day}"  # e.g., "2022-03-01"
                date_values.append((date_value, year, month, day, item['Key']))
                filtered_files.append(item['Key'])

# Define expected columns
expected_columns = [
    "center_code",
    "kbzpay_center_code",
    "state_of_kbzpay_center",
    "township_of_kbzpay_center",
    "center_open_date",
    "center_status",
    "center_status_start_date",
    "center_status_end_date",
    "center_size",
    "cash_out_start_date",
    "operating_hours",
    "operating_hours_change_start_date",
    "operating_hours_change_end_date",
    "service",
    "services_change_start_date",
    "services_change_end_date",
    "super_agent_short_code",
    "org_name",
    "retail_agent_short_code",
    "retail_short_code_change_start_date",
    "retail_short_code_change_end_date",
    "retail_agent_name",
    "retail_agent_joining_date",
    "remark"
]

for date_value, year, month, day, file_key in date_values:
    # Load file from S3
    df = spark.read.option("header", "true") \
        .option("inferSchema", "false") \
        .option("quotechar", '\\') \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("delimiter", ",") \
        .option("multiline", "true") \
        .option("wholeText", "true") \
        .csv(f"s3://{bucket}/{file_key}")

    # Step 1: Clean column names by stripping whitespace and handling newlines
    cleaned_columns = [
    c.replace("\n", "").replace("\r", "").strip().lower().replace(' ', '').replace('_ ', '_').replace(' _', '_')
    for c in df.columns
    ]
    df = df.toDF(*cleaned_columns)
    
    # Step 2: Trim leading and trailing spaces from each column
    # Check if the first column is '' or ' ' and rename it to 'center_code' only if it hasn't already been set to 'center_code'
    df_cleaned = df.select([
        trim(col(c)).alias('center_code') if (i == 0 and (c.strip() == '' or c.strip() == ' ') and c != 'center_code') 
        else trim(col(c)).alias(c)
        for i, c in enumerate(df.columns)
    ])

    # Step 3: Replace newlines and carriage returns with a single space
    df_cleaned = df_cleaned.select([
        regexp_replace(col(c), r'[\n\r]+', ' ').alias(c) for c in df_cleaned.columns
    ])
    
    # Step 4: Replace multiple spaces within the text with a single space
    df_cleaned = df_cleaned.select([
        regexp_replace(col(c), r'\s+', ' ').alias(c) for c in df_cleaned.columns
    ])
    
    # Drop unwanted columns if they exist
    columns_to_drop = ['_c24', '_c25', '_c26', '_c27', '_c28', '_c29', '_c30', '_c31', '_c32', '_c33']
    df_cleaned = df_cleaned.drop(*[col for col in columns_to_drop if col in df_cleaned.columns])
    
    #validate_columns(df_cleaned, expected_columns)

    # Add year_key, month_key, and day_key using PySpark DataFrame
    df_transformed = df_cleaned \
        .withColumn("datadate", lit(date_value)) \
        .withColumn("year_key", lit(int(year))) \
        .withColumn("month_key", lit(int(year + month))) \
        .withColumn("day_key", lit(int(year + month + day)))

    # SQL Query to process the data
    ChangeSchema_node = DynamicFrame.fromDF(
        df_transformed.select(
            col("datadate").cast("string").alias("datadate"),
            col("center_code").cast("string").alias("center_code"),
            col("kbzpay_center_code").cast("string").alias("kbzpay_center_code"),
            col("state_of_kbzpay_center").cast("string").alias("state_of_kbzpay_center"),
            col("township_of_kbzpay_center").cast("string").alias("township_of_kbzpay_center"),
            col("center_open_date").cast("string").alias("center_open_date"),
            col("center_status").cast("string").alias("center_status"),
            col("center_status_start_date").cast("string").alias("center_status_start_date"),
            col("center_status_end_date").cast("string").alias("center_status_end_date"),
            col("center_size").cast("string").alias("center_size"),
            col("cash_out_start_date").cast("string").alias("cash_out_start_date"),
            col("operating_hours").cast("string").alias("operating_hours"),
            col("operating_hours_change_start_date").cast("string").alias("operating_hours_change_start_date"),
            col("operating_hours_change_end_date").cast("string").alias("operating_hours_change_end_date"),
            col("service").cast("string").alias("service"),
            col("services_change_start_date").cast("string").alias("services_change_start_date"),
            col("services_change_end_date").cast("string").alias("services_change_end_date"),
            col("super_agent_short_code").cast("string").alias("super_agent_short_code"),
            col("org_name").cast("string").alias("org_name"),
            col("retail_agent_short_code").cast("string").alias("retail_agent_short_code"),
            col("retail_short_code_change_start_date").cast("string").alias("retail_short_code_change_start_date"),
            col("retail_short_code_change_end_date").cast("string").alias("retail_short_code_change_end_date"),
            col("retail_agent_name").cast("string").alias("retail_agent_name"),
            col("retail_agent_joining_date").cast("string").alias("retail_agent_joining_date"),
            col("remark").cast("string").alias("remark"),
            col("year_key").cast("int").alias("year_key"),
            col("month_key").cast("int").alias("month_key"),
            col("day_key").cast("int").alias("day_key")
        ), 
        glueContext, 
        "ChangeSchema_node"
    )

    # SqlQuery0 = '''
    # SELECT * FROM myDataSource 
    # '''
    # SQLQuery_node1730101456494 = sparkSqlQuery(glueContext, query=SqlQuery0, mapping={"myDataSource": ChangeSchema_node}, transformation_ctx="SQLQuery_node1730101456494")

    # Write the results to S3
    AmazonS3_node1730101467610 = glueContext.getSink(path="s3://acoe-datalake-processed/lookup/pay_center/", connection_type="s3", updateBehavior="LOG", partitionKeys=["year_key", "month_key", "day_key"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1730101467610")
    AmazonS3_node1730101467610.setCatalogInfo(catalogDatabase="datalake-processed-lookup", catalogTableName="lookup_pay_center")
    AmazonS3_node1730101467610.setFormat("glueparquet", compression="snappy")
    AmazonS3_node1730101467610.writeFrame(ChangeSchema_node)


job.commit()
