import boto3
import json
import pandas as pd
def json_df():

    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    bucket_name = 'data-249-final-project'
    directory_prefix = 'Talent/'
    df_array = []

    try:
        response = s3_client.list_objects(Bucket=bucket_name, Prefix=directory_prefix)
        if 'Contents' in response:
            json_objects = [obj for obj in response['Contents'] if obj['Key'].endswith('.json')]
            for json_object in json_objects:
                response = s3_resource.Object(bucket_name, json_object['Key']).get()
                json_content = response['Body'].read()

                data = json.loads(json_content)
                df_array.append(data)

                # Test here to check that is correctly extracted in proper Data frame format
                if isinstance(data, list):  # Check if the JSON data is a list of dictionaries
                    df = pd.DataFrame(data)
                    print(df)
                elif isinstance(data, dict):  # Check if the JSON data is a single dictionary
                    df = pd.DataFrame([data])
                    print(df)
                else:
                    print(f"Unsupported JSON format in '{json_object['Key']}'")

        else:
            print(f"No objects found in the '{directory_prefix}' directory.")
    except Exception as e:
        print(f"Error: {e}")

    # Test to check count of extracted files is same as expected (is it one to one)
    # Test that data frames are not empty
    # Test expected number of columns and rows
    #print(df_array)