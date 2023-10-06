from main import *
talent_df = main()

# import sqlalchemy
# from sqlalchemy import create_engine
# import pandas as pd
# import pprint as pp
# import numpy as np
#
# server_name = 'your-server-name'
# database_name = 'your-database-name'
# username = 'your_username'
# password = 'your_password'
#
# connection_string = f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server"
# engine = create_engine(connection_string)

# df.to_sql(name='table_name', con=engine, if_exists='replace', index=False)

# col = talent_df.columns
# print(col)

def create_csv_df(df, column_name, custom_order=None):
    # Data Frame to store column name
    ordered_df = pd.DataFrame()
    ordered_df[column_name] = df[column_name].drop_duplicates()
    # Keeping only distinct values
    ordered_df = ordered_df.dropna().reset_index(drop=True)
    # Dropping NaN and resetting index

    if custom_order is not None:
        # this is specifically for degree as I wanted to order 1st, 2:1, 2:2 and 3rd in that order
        # when calling this function, user must specify a custom order they want as a variable
        ordered_df['ordered_' + column_name] = ordered_df[column_name].map(custom_order)
        ordered_df = ordered_df.sort_values(by='ordered_' + column_name)
        ordered_df = ordered_df.drop(columns='ordered_' + column_name).reset_index(drop=True)

    ordered_df[f'{column_name}_id'] = ordered_df.index + 1
    # adding an index inside the Data Frame
    desired_order = [f'{column_name}_id', column_name]
    # ordering the columns
    ordered_df = ordered_df[desired_order]

    return ordered_df


uni_df = create_csv_df(talent_df, 'uni')
print(uni_df)
custom_degree_order = {'1st': 1, '2:1': 2, '2:2': 3, '3rd': 4}
degree_df = create_csv_df(talent_df, 'degree', custom_degree_order)
print(degree_df)
city_df = create_csv_df(talent_df, 'city')
print(city_df)

print(talent_df)
