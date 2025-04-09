# Define test data
data = [
    ("2025-03-28", 437, 437, "complete", "2025-03-26 23:48:09")
]

# Define schema
columns = ["branch_date", "no_of_total_branch", "no_of_eoc_branch", "status", "start_time"]

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#-------------------------------------------------------------------------------- Vers 1 ---------------------------------------------------------------------------------------------- 

while True:
    # Check if status is 'complete'
    df_status=check_eoc_status()

    # Today's date 
    today_date = datetime.now(timezone('Asia/Yangon')).date()
    
    # Write data to S3
    write_to_s3(glueContext, df_status)
    
    if df_status is not None and df_status.filter((F.col("status") == "complete") & (F.col("branch_date") >= today_date)).count() > 0:
        print("EOC process is complete. Exiting the loop.")
        df_status.show(5)
        break
    else:
        print("EOC process is not complete. Waiting for 5 minutes before retrying...")
        time.sleep(300)  # Wait for 5 minutes before checking again

#-------------------------------------------------------------------------------- Vers 2 ---------------------------------------------------------------------------------------------- 


while True:
    # Check if status is 'complete'
    df_status=check_eoc_status()

    # Today's date 
    today_date = datetime.now(timezone('Asia/Yangon'))
    
    # Write data to S3
    write_to_s3(glueContext, df_status)
    
    if df_status is not None and df_status.filter((F.col("status") == "complete") & (F.col("branch_date") == today_date)).count() > 0:
        print("EOC process is complete. Exiting the loop.")
        df_status.show(5)
        break
    else:
        print("EOC process is not complete. Waiting for 5 minutes before retrying...")
        time.sleep(300)  # Wait for 5 minutes before checking again

#-------------------------------------------------------------------------------- Vers 3 ---------------------------------------------------------------------------------------------- 

while True:
    # Check if status is 'complete'
    df_status=check_eoc_status()

    # Today's date 
    today_date = datetime.now(timezone('Asia/Yangon'))
    
    # Write data to S3
    write_to_s3(glueContext, df_status)
    
    if df_status is not None and df_status.filter((F.col("status") == "complete") & (F.col("branch_date") < today_date)).count() > 0:
        print("EOC process is complete. Exiting the loop.")
        df_status.show(5)
        break
    else:
        print("EOC process is not complete. Waiting for 5 minutes before retrying...")
        time.sleep(300)  # Wait for 5 minutes before checking again