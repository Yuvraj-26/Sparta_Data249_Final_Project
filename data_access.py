import boto3
import pandas as pd
import json
import io


def resource_connection(bucket):
    s3_resource = boto3.resource('s3')
    my_bucket = s3_resource.Bucket(bucket)
    return my_bucket


def get_keys_txt(my_bucket, prefix, file_type='txt'):
    keys = []
    for obj in my_bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith(file_type):
            keys.append(obj.key)
    return keys


def read_txt(bucket):
    all_text = []
    for obj in bucket.objects.all():
        if obj.key.endswith('txt'):
            file = obj.get()['Body'].read().decode()
            lines = file.split('\n')
            remove_3 = lines[3:-1]
            all_text.append(remove_3)
    return all_text


def read_csv(bucket, prefix):
    all_text = pd.DataFrame()
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key.endswith('csv'):
            file = obj.get()['Body'].read().decode()
            df = pd.read_csv(io.StringIO(file))
            all_text = pd.concat([all_text, df])
    return all_text


def write_to_dataframe(file):
    list_person_dict = []
    for item in file:
        for line in item:
            name = line[:line.index(" -  ")].lower().rstrip()
            score_lines = line[line.index(" -  "):].split()
            psychometric_score = score_lines[-3][:score_lines[-3].index('/')]
            presentation_score = score_lines[-1][:score_lines[-1].index('/')]
            person_dict = {'name': name,
                           'psychometric_score': psychometric_score,
                           'presentation_score': presentation_score}
            list_person_dict.append(person_dict)
    df = pd.DataFrame(list_person_dict)
    return df


def json_df():
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    bucket_name = 'data-249-final-project'
    directory_prefix = 'Talent/'
    df_array = []

    try:
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

            else:
                print(f"No objects found in the '{directory_prefix}' directory.")
    except Exception as e:
        print(f"Error: {e}")

    return df_array
