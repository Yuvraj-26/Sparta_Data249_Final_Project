from transform_dfs import *
from main import *
from load_dataframe import *
import pandas as pd
talent_df = main()


transform_csv_talent(talent_df)
talent_df = talent_df.reset_index(drop=True)
talent_df['talent_id'] = talent_df.index + 1
col = talent_df.columns
# print(col)
desired_order = ['talent_id', 'first_name', 'last_name', 'gender', 'dob', 'email', 'city', 'house_number', 'street_name', 'postcode', 'phone_number', 'uni', 'degree', 'invited_date', 'month', 'invited_first_name', 'invited_last_name']
talent_df = talent_df[desired_order]
print(talent_df)

uni_df = create_csv_df(talent_df, 'uni')
custom_degree_order = {'1st': 1, '2:1': 2, '2:2': 3, '3rd': 4}
degree_df = create_csv_df(talent_df, 'degree', custom_degree_order)
city_df = create_csv_df(talent_df, 'city')

merged_df = talent_df.merge(uni_df, how='left', on='uni')
merged_df = merged_df.merge(degree_df, how='left', on='degree')
merged_df = merged_df.merge(city_df, how='left', on='city')

# drop the uni, degree, and city columns from the merged DataFrame
merged_df = merged_df.drop(columns=['uni', 'degree', 'city'])
#
# # Print the merged DataFrame
# # print(merged_df)
#
#
# columns_to_extract = ['talent_id', 'city_id', 'house_number', 'street_name', 'postcode']
# address_junction_df = merged_df[columns_to_extract]
# # print(address_junction_df)
#
# columns_to_extract = ['talent_id', 'uni_id', 'degree_id']
# uni_junction_df = merged_df[columns_to_extract]
# # print(uni_junction_df)
#
# recruiter_df = recruiter_id_df(talent_df)
# print(recruiter_df)