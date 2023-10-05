# import all necessary libraries & functions from the various modules
import data_access
import pandas as pd


# define main function, with all functions in it in ordered
def main():
    # connects to bucket with s3 resource
    bucket = data_access.resource_connection('data-249-final-project')
    # accesses txt files in bucket and converts to dataframe
    txt = data_access.read_txt(bucket)
    txt_df = data_access.write_to_dataframe(txt)
    # accesses csv files in specified directory and converts to df
    talent_csv_df = data_access.read_csv(bucket, 'Talent')
    academy_csv_df = data_access.read_csv(bucket, 'Academy')
    # accesses JSON files and converts to df
    json_df = data_access.json_df()
    return txt_df, talent_csv_df, academy_csv_df, json_df


# call the required df using indexing
df = main()[0]
