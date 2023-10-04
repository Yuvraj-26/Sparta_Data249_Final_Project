import json
import pandas as pd
import boto3

pd.set_option('display.max_columns', None)

# Function to list each tech language skill in JSON as new column with key as column label and value as cell in row
def expand_tech_self_score(df):
    # Check 'tech_self_score' column exists in our pre-created data frames and that it contains dictionaries
    if 'tech_self_score' in df and all(isinstance(item, dict) for item in df['tech_self_score']):
        # Split dictionary into separate columns
        tech_self_score_df = pd.DataFrame(df['tech_self_score'].tolist())

        # Add the new columns to pre-existing data frame (now languages are last columns)
        df = pd.concat([df, tech_self_score_df], axis=1)

        # Drop the original 'tech_self_score' column since it has been replaced will the new columns
        df.drop(columns=['tech_self_score'], inplace=True)
    return df


def expand_strength_weakness(df):
    # Check strength and weakness columns exits in the pre-existing data frame
    if 'strengths' in df and all(isinstance(item, list) for item in df['strengths']):
        # Create separate columns for each item in list
        strength_df = pd.DataFrame(df['strengths'].tolist(),
                                   columns=[f'strengths {i + 1}' for i in range(len(df['strengths'][0]))])

        # Add the new columns to the pre-existing data frame
        df = pd.concat([df, strength_df], axis=1)

        # Drop original strength column
        df.drop(columns=['strengths'], inplace=True)

    if 'weaknesses' in df and all(isinstance(item, list) for item in df['weaknesses']):
        # Create separate columns for each listy item
        weakness_df = pd.DataFrame(df['weaknesses'].tolist(),
                                   columns=[f'weaknesses {i + 1}' for i in range(len(df['weaknesses'][0]))])

        # Add new weakness columns to ore-existing data frame
        df = pd.concat([df, weakness_df], axis=1)

        # Droporiginal 'weakness' column
        df.drop(columns=['weaknesses'], inplace=True)
    return df

'''
 Function to split 'name' column into 'first_name' and 'last_name'
def split_name_column(df):
    if 'name' in df.columns:
        df[['first_name', 'last_name']] = df['name'].str.split(' ', 1, expand=True)
        df = df[['first_name', 'last_name'] + [col for col in df.columns if col != 'name']]
    return df
    '''

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


if __name__ == "__main__":
    example_dfs = json_df()

    # Call function to split tech skill score column
    processed_dfs = []
    for df in example_dfs:
        processed_df = expand_tech_self_score(df)
        processed_df = expand_strength_weakness(processed_df)
        processed_dfs.append(processed_df)

    # Show and label each new data frame
    for i, df in enumerate(processed_dfs):
        print(f"Person {i + 1}:")
        print(df)
        print()

