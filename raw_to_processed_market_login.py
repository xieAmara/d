import sys
import boto3
import pytz
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta
from pyspark.sql.functions import year, month, lit, when, regexp_replace, col, lag, monotonically_increasing_id, split, size, lit, expr, trim
from pyspark.sql.types import StructType, StructField, StringType
from pytz import timezone
from awsglue.dynamicframe import DynamicFrame 
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Function to delete duplicated files 
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

# Loop all the files in the folder 
if 'Contents' in response:
    for obj in response['Contents']:
        key = obj['Key']
        filename = key.replace(prefix, '') 

        # Retrieve each individual file and extract the day,month and year key 
        if filename.endswith('.csv'):
            s3_path = f"s3://{bucket_name}/{key}"
            
            filename = s3_path.split("/")[-1].split(".")[0]
            date = filename[-8:]
            date_obj = datetime.strptime(date, '%Y%m%d')
            prev_day = date_obj - timedelta(days=1)

            day_key = prev_day.strftime('%Y%m%d') 
            month_key = prev_day.strftime('%Y%m') 
            year_key = prev_day.strftime('%Y')

            # Read the raw s3 file 
            df = spark.read.text(s3_path) 

            # split all the values as per "," delimiter and placei t in the "value" column (unless if the whole row is empty)
            df_split = df.withColumn(
                "columns",
                F.when(F.col("value").isNotNull() & (F.trim(F.col("value")) != ""), 
                       F.split(F.trim(F.col("value")), ","))
                .otherwise(F.array())  # Ensures blank rows become an empty array
            )
            
            # Create five columns (strictly) through the split values 
            df_split = df_split.withColumn(
                "columns",
                expr("IF(size(columns) < 5, array_union(columns, array_repeat(NULL, 5 - size(columns))), columns)")
            )

            # Give each column a name 
            df = df_split.select(
                col("columns")[0].alias("Customer Id"),
                col("columns")[1].alias("Phone number"),
                col("columns")[2].alias("Customer Name"),
                col("columns")[3].alias("Login Time"),
                col("columns")[4].alias("col4")
            )
            
            # Remove the header of the file 
            df = df.filter(
                ~(
                    (col("Customer Id") == "Customer Id") &
                    (col("Phone number") == "Phone number") &
                    (col("Customer Name") == "Customer Name") &
                    (col("Login Time") == "Login Time") 
                )
            )
            
            # first drop all rows that as NULL 
            df_cleaned = df.dropna(how="all")
            
            # Identify where there is a delimiter value and solve 
            # Drop the error column 
            df_cleaned = df_cleaned.withColumn(
                "Customer Name", 
                F.when(
                    F.col("Login Time").isNotNull() & (~F.col("Login Time").rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")),
                    F.concat(F.col("Customer Name"), F.lit(" "), F.col("Login Time"))
                ).otherwise(F.col("Customer Name"))
            ).withColumn(
                "Login Time",
                F.when(
                    F.col("col4").isNotNull() & (F.col("col4") != ""),
                    F.col("col4")
                ).otherwise(F.col("Login Time"))
            ).drop("col4")

            df_cleaned = df_cleaned.withColumn("row_id", F.monotonically_increasing_id())
            
            # Create lag columns for the next row's values
            df_cleaned = df_cleaned.withColumn("next_customerid", F.lead("Customer Id", 1).over(Window.orderBy("row_id"))) \
                                    .withColumn("next_phonenumber", F.lead("Phone number", 1).over(Window.orderBy("row_id"))) \
                                    .withColumn("next_customername", F.lead("Customer Name", 1).over(Window.orderBy("row_id"))) \
                                   .withColumn("next_logintime", F.lead("Login Time", 1).over(Window.orderBy("row_id"))) 
                                   
            
            # Clean the columns by shifting values 
            df_cleaned = df_cleaned.withColumn(
                "Customer Name", 
                F.when(
                    (F.col("Login Time").isNull()) & 
                    (F.length(F.col("next_phonenumber")) > 18) & 
                    (F.col("next_customername").isNull()) & 
                    (F.col("next_logintime").isNull()), 
                    F.concat(F.col("Customer Name"), F.lit(" "), F.col("next_customerid"))
                ).otherwise(F.col("Customer Name"))
            )
            
            df_cleaned = df_cleaned.withColumn(
                "Login Time", 
                F.when(
                    (F.col("Login Time").isNull()) & 
                    (F.length(F.col("next_phonenumber")) > 10) & 
                    (F.col("next_customername").isNull()) & 
                    (F.col("next_logintime").isNull()), 
                    F.col("next_phonenumber")
                ).otherwise(F.col("Login Time"))
            )
            
            # Drop the row_id and other temporary columns
            df_cleaned = df_cleaned.drop("row_id", "next_customerid", "next_phonenumber", "next_logintime", "next_customername")
            
            # Drop the columns that were fixed 
            df_cleaned = df_cleaned.filter(
                ~(F.col("Customer Name").isNull() & F.col("Login Time").isNull())
            )
            
            # Create column for partionining 
            df = df_cleaned \
            .withColumn("file_name", lit(filename))\
            .withColumn("year_key",lit(year_key))\
            .withColumn("month_key", lit(month_key))\
            .withColumn("day_key", lit(day_key))

            # Insert the two columns to keep the schema presistent upon all years 
            if "Source" not in df.columns and "source" not in df.columns: 
                df = df.withColumn("source", lit(None))
            if "Role" not in df.columns and "role" not in df.columns:
                df = df.withColumn("role",lit(None))
            
                    
            df = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
            s3_client = boto3.client("s3")
            s3_bucket = "acoe-silver-layer"
            s3_prefix = f"market_customerlogin/processed/year_key={year_key}/month_key={month_key}/day_key={day_key}/"
            
            # Apply normal formatting 
            changecolumnname_node = ApplyMapping.apply(
                frame=df,
                mappings=[
                    ("customer id", "string", "customer_id", "string"),
                    ("phone number", "string", "phone_number", "string"), 
                    ("customer name", "string", "customer_name", "string"),
                    ("login time", "string", "login_time", "timestamp"),
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