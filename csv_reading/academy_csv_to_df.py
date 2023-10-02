import boto3
import pandas as pd
import pprint as pp

s3_client = boto3.client('s3')
bucket_name = 'data-249-final-project'
prefix = 'Academy'
all_objects = []

# Start with an empty continuation token
continuation_token = None

while True:
    if continuation_token:
        # Use the continuation token from the previous response
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token)
    else:
        # First call without continuation token
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    # Add the objects from the current response to the list
    all_objects.extend(response.get('Contents', []))
    # Check if there are more objects to fetch
    if response.get('IsTruncated'):
        continuation_token = response['NextContinuationToken']
    else:
        break

all_data = []

for obj in all_objects:
    response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
    df1 = pd.read_csv(response['Body'])
    all_data.append(df1)

academy_df = pd.concat(all_data, ignore_index=True)
print(academy_df)