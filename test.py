# import re

# data = """202000000002850480,09402740266,ma lwin lwin 
# mar,2022-11-10 16:00:55"""

# lines = data.strip().splitlines()
# result = []
# combined_line = ""

# for line in lines:
#     if "ma lwin lwin" in line:
#         combined_line = line.strip() + " "
#     elif combined_line:  
#         combined_line += line.strip()
#         result.append(combined_line)
#         combined_line = ""  
#     else:
#         result.append(line.strip())

# print(result)

# lines = data.strip().splitlines()
# result = []
# combined_line = ""

# for line in lines:
#     if "DAW IN" in line:
#         combined_line = line.strip() + " "
#     elif combined_line: 
#         combined_line += line.strip() 
#         result.append(combined_line) 
#         combined_line = ""  
#     else:
#         result.append(line.strip()) 

# if combined_line:
#     result.append(combined_line)

    
# print(result)

import re

def check_line_datetime_format(line):
  date_time_format = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
  return bool(re.search(date_time_format, line))

# def process_line_break(df):
#     processed_data = []
#     i = 0
#     while i < len(df):
#         row_str = ",".join(map(str, df.iloc[i].values)) + "\n"  
#         if not check_line_datetime_format(row_str.strip()[-19:]):
#             if i + 1 < len(df):
#                 next_row_str = ",".join(map(str, df.iloc[i + 1].values)) + "\n"
#                 combined_str = row_str.strip() + "\n" + next_row_str.strip()
#                 processed_data.extend(remove_line_break(combined_str))
#                 i += 2
#             else:
#                 processed_data.extend(remove_line_break(row_str.strip()))
#                 i += 1
#         else:
#             processed_data.append(row_str.strip())
#             i += 1
#     return processed_data


data = """202000000002850480,09402740266,ma lwin lwin 
mar,2022-11-10 16:00:55"""

data1 = """202000000002518816,09450111587,DAW IN

DAW TIN TIN NWE,2022-11-09 15:12:26
202000000002518816,09450111587,DAW IN

DAW TIN TIN NWE,    """

def remove_line_break(data):
    line_list = data.splitlines()       
    final = []
    combined = ""

    for line in line_list: 
        if not check_line_datetime_format(line[-19:]): 
            combined = line.strip() + " "
        elif combined: 
            combined += line.strip()
            final.append(combined) 
            combined = ""
        else: 
            final.append(combined)

    print(final)

# Identify line breaks 
def isLineBreak(df):
    rows = df.rdd.collect()
    lines_to_combine = []
    i = 0

    while i < len(rows):
        row = rows[i]
        login_time = row["Login Time"]

        if login_time is None:
            if i + 1 < len(rows):
                next_row = rows[i + 1]
                customer_id = next_row["Customer Id"]
                phone_number = next_row["Phone number"]
                
                if customer_id is not None and not re.search(r'\d', str(customer_id)) and phone_number is not None and re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', str(phone_number)):
                    lines_to_combine.append((row, next_row))
                    i += 2
                else:
                    i += 1
            else:
                i += 1
        else:
            i += 1

    return lines_to_combine


# Add data with line breaks to a list 


remove_line_break(data)
remove_line_break(data1)

# --------------------------------------------------------------------------------------------------------------------------------


import sys
import boto3
import pytz
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta
from pyspark.sql.functions import year, month, lit, when
from pyspark.sql.types import StringType 
from pytz import timezone
from awsglue.dynamicframe import DynamicFrame 
from pyspark.sql.functions import col

from awsglue.context import GlueContext
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

source = "s3://acoe-silver-layer/market_customerlogin/source/Customer_Login_Log_of_KBZPay_Market_20221111.csv"
dest = "s3://acoe-silver-layer/test_intern/"

file = "Customer_Login_Log_of_KBZPay_Market_20221111.csv"

custom_options = {
    "header": True,
    "inferSchema": False,
}

dynf = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, 
    connection_type="s3", 
    format="csv", 
    connection_options={"paths": [source]}, 
    transformation_ctx="dynf")

df = dynf.toDF()
df.show()
def isLineBreak(df):
    rows = df.rdd.collect()
    lines = []
    i = 0

    while i < len(rows):
        row = rows[i]
        login_time = row["Login Time"]

        if login_time is None:
            if i + 1 < len(rows):
                next_row = rows[i + 1]
                customer_id = next_row["Customer Id"]
                phone_number = next_row["Phone number"]
                
                if customer_id is not None and not re.search(r'\d', str(customer_id)) and phone_number is not None and re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', str(phone_number)):
                    lines.append((row, next_row))
                    i += 2
                else:
                    i += 1
            else:
                i += 1
        else:
            i += 1

    return lines
print(isLineBreak(df))
def to_csv(row_tuples):
    csv_strings = []
    for row_pair in row_tuples:
        row1 = row_pair[0]
        row2 = row_pair[1]

        row1_values = ",".join(str(value) for value in [v for v in row1.asDict().values() if v is not None])
        row2_values = ",".join(str(value) for value in [v for v in row2.asDict().values() if v is not None])

        csv_strings.append(row1_values)
        csv_strings.append(row2_values)

    return csv_strings
print(to_csv(isLineBreak(df)))
import re

def check_line_datetime_format(line):
    date_time_format = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
    return bool(re.search(date_time_format, str(line).strip()))

def remove_line_break(data):
    line_list = data  
    final = []
    combined = ""

    for line in line_list:
        if not check_line_datetime_format(line.strip()[-19:]):
            combined = line.strip() + " "
        elif combined:
            combined += line.strip()
            final.append(combined)
            combined = ""
        else:
            if combined:
                final.append(combined)
            final.append(line.strip())
            combined = ""
    return final

remove_line_break(to_csv(isLineBreak(df)))
job.commit()
    
#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

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
        (F.col("next_customername").isNull()) &
        (F.col("next_logintime").isNull()),
        F.concat(F.col("Customer Name"), F.lit(" "), F.col("next_customerid"))
    ).otherwise(F.col("Customer Name"))
)

df_cleaned = df_cleaned.withColumn(
    "Login Time",
    F.when(
        (F.col("Login Time").isNull()) &
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
