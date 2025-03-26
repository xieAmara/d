import json
import boto3
import os
from io import StringIO
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

current_date = datetime.utcnow() + timedelta(hours=6, minutes=30)
surfix = current_date.strftime('%Y%m%d')
year = current_date.strftime('%Y')
month = current_date.strftime('%Y%m')
day = current_date.strftime('%Y%m%d')


def get_secret(secret_name):
    # Create a Secrets Manager client
    client = boto3.client('secretsmanager')

    try:
        # Fetch the secret from AWS Secrets Manager
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)

    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

def get_google_credentials():
    creds = None
    try:
        token_info = get_secret("gsheet/token.json")
        creds = Credentials.from_authorized_user_info(token_info, SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
    except Exception as e:
        print(f"Error creating credentials from token_info: {e}")
        creds = None

    return creds
    
def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    spreadsheet_id = event.get('sheet_id','')   # '1pK3sftZj8EL1OykgBA1CbQgzNJtX2CZhnJt_6OK1sPI'
    range_name = event.get('sheet_range','')       #'Point to Discuss with BU!A2:B4'
    bucketName = event.get('bucket_name','')
    bucketPath = event.get('bucket_path','')
    fileName = event.get('file_name','')
    
    try:

        def get_google_sheet_data(spreadsheet_id, range_name):
            
            # Initialize creds
            creds = get_google_credentials()
            
            if not creds:
                print("No valid credentials available.")
                return {"statusCode": 500, "body": "No valid credentials available."}
                
            try:
                service = build("sheets", "v4", credentials=creds)
                sheet = service.spreadsheets()
                result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
                values = result.get("values", [])
    
                if not values:
                    print("No data found.")
                    return None
    
                # Use the first row as column headers
                headers = values[0]
                
                # Create an in-memory text stream to hold the JSON data
                output = StringIO()
            
                # Process the remaining rows
                for row in values[1:]:
                  # Create a dictionary for each row using the headers as keys
                  row_dict = {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                  # Convert the dictionary to a JSON object
                  json_data = json.dumps(row_dict)
                  # Print the JSON object
                  #print(json_data)
                  
                  # Write json data
                  output.write(json_data + '\n')
                  
                # Move to the beginning of the stream
                output.seek(0) 
                return output
    
            except HttpError as err:
                print(err)
                return None
        
        gsheet = get_google_sheet_data(spreadsheet_id, range_name)
        print(gsheet)
        
        # Specify the S3 bucket and the file name
        partition = f"year={year}/month={month}/day={day}"  
        file_name = f"{bucketPath}/{partition}/{fileName}_{surfix}.json"
        
        # Upload the file to S3
        s3_client.put_object(Bucket=bucketName, Key=file_name, Body=gsheet.getvalue())
        
        return {"statusCode": 200, "body": "Data uploaded successfully."}
        
    except Exception as e:
        
        return {
            'statusCode': 500,
            'body': json.dumps(f"{e}")
        }