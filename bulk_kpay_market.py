import sys
import boto3
import io
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
  
def delete_s3_files(bucket, prefix):
    objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in objects_to_delete:
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": obj["Key"]} for obj in objects_to_delete["Contents"]]}
        )
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

s3 = boto3.client('s3')
source = "s3://acoe-silver-layer/KBZpay_market/source/"
dest = "s3://acoe-silver-layer/KBZpay_market/processed/"

bucket_name = "acoe-silver-layer"
prefix = "KBZpay_market/"

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
            df = spark.read.options(**custom_options).csv(s3_path)
            file_name = s3_path.split("/")[-1]
            date = file_name.split("_")[-1].split(".")[0]
            date_obj = datetime.strptime(date, '%d%m%Y')
            prev_day = date_obj - timedelta(days=1)
                                         
            day_key = prev_day.strftime('%Y%m%d') 
            month_key = prev_day.strftime('%Y%m') 
            year_key = prev_day.strftime('%Y')
          
            df = df \
            .withColumn("file_name", lit(file_name))\
            .withColumn("year_key",lit(year_key))\
            .withColumn("month_key", lit(month_key))\
            .withColumn("day_key", lit(day_key))

            df = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
            s3_bucket = "acoe-silver-layer"
            s3_prefix = f"KBZpay_market/processed/year_key={year_key}/month_key={month_key}/day_key={day_key}/"

            changecolumnname_node = ApplyMapping.apply(
                frame=df,
                mappings=[
                    ("level 1 category id", "string", "level_1_category_id", "string"), 
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
                    ("day_key", "string", "day_key", "string"),
                ], 
                transformation_ctx="changecolumnname_node",
            )

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
                mapping = {"myDataSource":changecolumnname_node}, 
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