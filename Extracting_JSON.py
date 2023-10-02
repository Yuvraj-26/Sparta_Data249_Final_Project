import boto3
import json
import pandas as pd

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

bucket_name = 'data-249-final-project'
directory_prefix = 'Talent/'

try:
    response = s3_client.list_objects(Bucket=bucket_name, Prefix=directory_prefix)
    if 'Contents' in response:
        json_objects = [obj for obj in response['Contents'] if obj['Key'].endswith('.json')]
        for json_object in json_objects:
            response = s3_resource.Object(bucket_name, json_object['Key']).get()

            df = pd.read_csv(response['Body'])
            print(df)

    else:
        print(f"No objects found in the '{directory_prefix}' directory.")
except Exception as e:
    print(f"Error: {e}")

