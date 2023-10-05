import pandas as pd
import numpy as np
import re

def convert_to_int(df, column_name):
    df[column_name] = df[column_name].astype('int')

def split_column_on_space(df, column_to_split, name1_column, name2_column):
    df[[name1_column, name2_column]] = df[column_to_split].apply(lambda x: pd.Series(str(x).split(" ", 1)))
    column_to_split_index = df.columns.get_loc(column_to_split)
    df.drop(columns=column_to_split, inplace=True)
    name2_pop = df.pop(name2_column)
    df.insert(column_to_split_index, name2_column, name2_pop)
    name1_pop = df.pop(name1_column)
    df.insert(column_to_split_index, name1_column, name1_pop)
    return df

def lower_case_columns(df):
    df.columns = df.columns.str.lower()
    return df

def convert_rows_to_lower(df, column_name):
    df[column_name] = df[column_name].str.lower()
    return df

def reverse_date(df, column_name):
    date_split = df[column_name].str.split('/', expand=True)
    df[column_name] = date_split[2] + '/' + date_split[1] + '/' + date_split[0]
    return df

def convert_column_to_date(df, column_name):
    df[column_name] = pd.to_datetime(df[column_name])
    return df

def replace_strings_in_column(df, column, to_replace, replace_with):
    df[column] = df[column].replace(to_replace, replace_with)
    return df

def convert_nan_to_0(df, column):
    df[column] = df[column].fillna(0)
    return df

def convert_all_nan_to_0(df):
    df.replace(np.nan,0)
    return df

def convert_rows_to_string(df, column):
    df[column] = df[column].astype(str)
    return df

def merge_columns(df, column1, column2):
    df[column1] = df[column1] + df[column2]
    return df

def convert_all_0_to_nan(df):
    df.replace(0,np.nan)
    return df

def convert_column_to_date_full(df, column):
    df[column] = pd.to_datetime(df[column], format='%d%B %Y')

# Clean and standardize UK phone numbers
def clean_uk_phone_number(phone):
    if pd.notna(phone) and phone != 'Invalid':
        # Remove non-numeric characters
        phone = re.sub(r'\D', '', str(phone))
        # Apply the desired format (+44 20 1234 5678 or 020 1234 5678)
        if len(phone) == 12:
            phone = f'+{phone[:2]} {phone[2:4]} {phone[4:8]} {phone[8:]}'
        elif len(phone) == 10:
            phone = f'0{phone[:1]} {phone[1:5]} {phone[5:]}'
    return phone

def apply_standard_uk_number_to_column(df, column):
    df[column] = df[column].apply(clean_uk_phone_number)
    return df

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