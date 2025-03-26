import json
import requests

def lambda_handler(event, context):
    """
    Retrieves data from a sample API.
    """
    try:
        # Replace with your API endpoint
        url = "https://api.example.com/data"
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        return {
            'statusCode': 200,
            'body': json.dumps(data)
        }
    except requests.exceptions.RequestException as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }