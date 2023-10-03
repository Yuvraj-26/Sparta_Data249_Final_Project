import boto3
import json
import pandas as pd

def json_df():

    s3_client = boto3.client('s3',
                             aws_access_key_id=return_key(),
                             aws_secret_access_key=return_secret_key()
                             )

    s3_resource = boto3.resource('s3',
                                 aws_access_key_id=return_key(),
                                 aws_secret_access_key=return_secret_key()
                                 )

    bucket_name = 'data-249-final-project'
    directory_prefix = 'Talent/'
    df_array = []
    all_objects = []

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory_prefix)

        continuation_token = None
        #Extend it to collect more than 1000 results
        while True:
            if continuation_token:
                # Use the continuation token from the previous response
                response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory_prefix,
                                                     ContinuationToken=continuation_token)
            else:
                # First call without continuation token
                response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory_prefix)
            # Add the objects from the current response to the list
            all_objects.extend(response.get('Contents', []))
            # Check if there are more objects to fetch
            if response.get('IsTruncated'):
                continuation_token = response['NextContinuationToken']
            else:
                break

            json_objects = [obj for obj in all_objects if obj['Key'].endswith('.json')]
            for json_object in json_objects:
                response = s3_resource.Object(bucket_name, json_object['Key']).get()
                json_content = response['Body'].read()
                data = json.loads(json_content)

                # Test here to check that is correctly extracted in proper Data frame format
                if isinstance(data, list):  # Check if the JSON data is a list of dictionaries
                    df = pd.DataFrame(data)
                elif isinstance(data, dict):  # Check if the JSON data is a single dictionary
                    df = pd.DataFrame([data])
                else:
                    print(f"Unsupported JSON format in '{json_object['Key']}'")

                df_array.append(df)

        else:
            print(f"No objects found in the '{directory_prefix}' directory.")
    except Exception as e:
        print(f"Error: {e}")

    # Test to check count of extracted files is same as expected (is it one to one)
    # Test that data frames are not empty
    # Test expected number of columns and rows
    # Concat the df's and save to a CSV
    df = pd.concat(df_array)
    df.to_csv('JSON_df.csv', index=False)


