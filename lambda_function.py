import json
import requests
import boto3
import json
import csv
import os
from io import StringIO

def is_csv_url(url):
    return url.lower().endswith('.csv')

def fetch_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def csv_to_json(csv_text):
    reader = csv.DictReader(StringIO(csv_text))
    return json.dumps([row for row in reader])

def upload_to_s3(data, filename, bucket_name):
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket_name,
        Key=filename,
        Body=data,
        ContentType='application/json'
    )

def lambda_handler(event, context):
    try:
        apis = {
            "holiday": "https://www.gov.uk/bank-holidays.json",
            "height_weight": "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv"
        }

        bucket_name = "ndataassignment"

        results = []
        for api_name, url in apis.items():
            try:
                raw_data = fetch_data(url)

                if is_csv_url(url):
                    json_data = csv_to_json(raw_data)
                else:
                    json_data = json.dumps(json.loads(raw_data))

                filename = f"{api_name}_{context.aws_request_id}.json"
                upload_to_s3(json_data, filename, bucket_name)

                results.append({
                    'api': api_name,
                    'status': 'success',
                    'filename': filename
                })

            except Exception as e:
                results.append({
                    'api': api_name,
                    'status': 'error',
                    'error': str(e)
                })

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing complete',
                'results': results
            })
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error occurred',
                'error': str(e)
            })
        }