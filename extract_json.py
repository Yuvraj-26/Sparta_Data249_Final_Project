import json
import pandas as pd
import boto3

pd.set_option('display.max_columns', None)

# MAIN FUNCTION TO EXTRACT AND CONVERT JSON FILE TO DATA FRAME
def json_df():
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    bucket_name = 'data-249-final-project'
    directory_prefix = 'Talent/'
    df_array = []

    try:
        # Use paginator to avoid only returning first 1000 objects
        paginator = s3_client.get_paginator('list_objects')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=directory_prefix):
            if 'Contents' in page:
                json_objects = [obj for obj in page['Contents'] if obj['Key'].endswith('.json')]
                for json_object in json_objects:
                    response = s3_resource.Object(bucket_name, json_object['Key']).get()
                    json_content = response['Body'].read()

                    data = json.loads(json_content)

                    # Create DF from JSON data
                    if isinstance(data, list):  # Check if the JSON data is a list of dictionaries
                        df = pd.DataFrame(data)
                        df_array.append(df)
                    elif isinstance(data, dict):  # Check if the JSON data is a single dictionary
                        df = pd.DataFrame([data])
                        df_array.append(df)
                    else:
                        print(f"Unsupported JSON format in '{json_object['Key']}'")

    except Exception as e:
        print(f"Error: {e}")

    return df_array