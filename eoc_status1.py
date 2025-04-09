import sys
import pandas as pd
import boto3
import pytz
import time
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from awsglue import DynamicFrame
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql import DataFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Get Today Date Time   
def get_current_date():
    today = datetime.now(tz=ZoneInfo("Asia/Yangon"))
    return today

#func: used to fetch data from Oracle DB and check eoc status
def check_eoc_status():
    # Script generated for node Oracle SQL
    sttm_branch_df = glueContext.create_dynamic_frame.from_options(
        connection_type="oracle",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "SFCUBS.STTM_BRANCH",
            "connectionName": "core-banking-kbzrptdcpdb-sfcubs",
        },
        transformation_ctx="Oracle_source"
    ).toDF()
    
    aetb_eoc_branches_df = glueContext.create_dynamic_frame.from_options(
        connection_type="oracle",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "SFCUBS.AETB_EOC_BRANCHES",
            "connectionName": "core-banking-kbzrptdcpdb-sfcubs",
        },
        transformation_ctx="Oracle_source"
    ).toDF()
    
    aetb_eoc_runchart_df = glueContext.create_dynamic_frame.from_options(
        connection_type="oracle",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "SFCUBS.AETB_EOC_RUNCHART",
            "connectionName": "core-banking-kbzrptdcpdb-sfcubs",
        },
        transformation_ctx="Oracle_source"
    ).toDF()
    
    sttm_dates_df = glueContext.create_dynamic_frame.from_options(
        connection_type="oracle",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "SFCUBS.STTM_DATES",
            "connectionName": "core-banking-kbzrptdcpdb-sfcubs",
        },
        transformation_ctx="Oracle_source"
    ).toDF()
    
    # Get total branches
    total_branch_df = sttm_branch_df.filter(F.col('record_stat') == 'O') \
        .agg(F.countDistinct('branch_code').alias('no_of_total_branch'))
    max_date_df = sttm_dates_df.select(F.max(F.col('today')).alias('branch_date'))


    # Process EOC branch
    eoc_branch_df = aetb_eoc_branches_df.filter(
        (aetb_eoc_branches_df['target_stage'] == 'POSTEOPD_3') &
        (F.col('branch_date')== max_date_df.select('branch_date').first()[0]) &
        (aetb_eoc_branches_df['eoc_status'] == 'C')
    ).groupBy('branch_date').agg(F.countDistinct('branch_code').alias('no_of_eoc_branch'))
    

    # Process EOC status
    eoc_status_df = total_branch_df.crossJoin(eoc_branch_df).withColumn(
        'status', F.when(F.col('no_of_total_branch') == F.col('no_of_eoc_branch'), 'complete').otherwise('incomplete')
    )

    # Process EOC time
    eoc_time_df = aetb_eoc_runchart_df.filter(
        (F.col('branch_date')== max_date_df.select('branch_date').first()[0]) &
        (aetb_eoc_runchart_df['eoc_stage'] == 'POSTEOPD_3') &
         (aetb_eoc_runchart_df['eoc_stage_status'] == 'C')
    ).groupBy('branch_date').agg(
        F.min('start_time').alias('start_time'),
        F.max('end_time').alias('end_time')
    )

    # Join eoc_status and eoc_time on branch_date
    final_df = eoc_status_df.join(eoc_time_df, 'branch_date', 'inner')
    final_df.show(5)
    final_df = final_df.withColumn('inserted_time', F.to_timestamp(lit(get_current_date().strftime('%Y-%m-%d %H:%M:%S')), 'yyyy-MM-dd HH:mm:ss'))
    final_df = final_df.withColumn("year_key",F.lit(get_current_date().strftime('%Y')))
    final_df = final_df.withColumn("month_key",F.lit(get_current_date().strftime('%Y%m')))
    final_df = final_df.withColumn("day_key",F.lit(get_current_date().strftime('%Y%m%d')))
    final_df = final_df.withColumn("hh_mm", F.date_format(F.to_timestamp(F.lit(get_current_date().strftime('%Y-%m-%d %H:%M:%S')), 'yyyy-MM-dd HH:mm:ss'), "HHmm"))

    return final_df

#func: write data to S3 and update data catalog table
def write_to_s3(glueContext: GlueContext, data_frame: DataFrame)-> None:
    final_dyf = DynamicFrame.fromDF(data_frame, glueContext, "final_dyf")
    S3_processed = glueContext.getSink(
    path="s3://acoe-data-engineering/cbs_daily_eoc_status/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year_key", "month_key", "day_key", "hh_mm"],
    enableUpdateCatalog=True,  # Update Glue Data Catalog
    transformation_ctx="S3_processed"
    )
    
    S3_processed.setCatalogInfo(
        catalogDatabase="datalake-processed-kbzrptdcpdb", 
        catalogTableName="cbs_daily_eoc_status"
    )
    S3_processed.setFormat("glueparquet", compression="snappy")
    S3_processed.writeFrame(final_dyf)