from extract_json import *
import pandas as pd
import matplotlib.pyplot as plt

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
        strength_df = pd.DataFrame(df['strengths'].tolist(), columns=[f'strengths {i + 1}' for i in range(len(df['strengths'][0]))])

        # Add the new columns to the pre-existing data frame
        df = pd.concat([df, strength_df], axis=1)

        # Drop original strength column
        df.drop(columns=['strengths'], inplace=True)

    if 'weaknesses' in df and all(isinstance(item, list) for item in df['weaknesses']):

        # Create separate columns for each listy item
        weakness_df = pd.DataFrame(df['weaknesses'].tolist(), columns=[f'weaknesses {i + 1}' for i in range(len(df['weaknesses'][0]))])

        # Add new weakness columns to ore-existing data frame
        df = pd.concat([df, weakness_df], axis=1)

        # Droporiginal 'weakness' column
        df.drop(columns=['weaknesses'], inplace=True)
    return df

def split_name_column(df):
    if 'name' in df.columns:
        df[['first_name','last_name']] = df['name'].str.split(n=1, expand=True)
        df.drop(columns=['name'], inplace=True)
    return df

def convert_yes_no_to_binary(df):
    binary_columns = ['self_development', 'geo_flex', 'financial_support_self']
    for column in binary_columns:
        df[column] = df[column].apply(lambda x: 1 if isinstance(x, str) and x.lower() == 'yes' else (0 if isinstance(x, str) and x.lower() == 'no' else x))
    return df

def convert_date(df):
    # Converts a date string to a date object.
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    return df

def process_example_dfs(example_dfs):
    processed_json_dfs = []
    for df in example_dfs:

        df = expand_tech_self_score(df)
        df = expand_strength_weakness(df)
        df = split_name_column(df)
        df = convert_yes_no_to_binary(df)
        df = convert_date(df)

        processed_json_dfs.append(df)
    combined_df = pd.concat(processed_json_dfs, ignore_index=True)

    desired_order = ['first_name', 'last_name', 'date', 'self_development', 'geo_flex', 'financial_support_self',
                     'result', 'course_interest', 'C#', 'C++', 'Java', 'Python', 'R', 'JavaScript', 'Ruby', 'SPSS',
                     'PHP', 'strengths 1', 'strengths 2', 'strengths 3', 'weaknesses 1', 'weaknesses 2', 'weaknesses 3']

    concatenated_df = combined_df[desired_order]
    return concatenated_df


if __name__ == "__main__":
    example_dfs = json_df()
    print(process_example_dfs(example_dfs))