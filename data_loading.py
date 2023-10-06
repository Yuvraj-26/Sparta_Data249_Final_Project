
import pyodbc
from sqlalchemy import create_engine
import pandas as pd

server = 'localhost,1433'
database = 'Final_Project' 
username = 'SA'
password = 'yourStrong(!)Password'
driver = 'ODBC Driver 17 for SQL Server'

pyodbc_conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
sqlalchemy_conn = sqlalchemy.create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}")
cursor = pyodbc_conn.cursor()


# list of dfs we have to work with, to have reference of their names

dfs = [txt_df, talent_csv_df, academy_csv_df, json_df]


# can create the df for the personal_info table by merging the talent & academy dfs

spartans_merged = talent_csv_df.merge(academy_csv_df, how='left', on='')
df1.merge(df2, how='inner', on='first_name')

personal_info_df = spartans_merged['first_name', 'last_name', 'gender', 'DOB', 'email']




# Creating dataframes for each of the tables, and renaming the columns so that they match those of the SQL table

# trainers table can be made by finding the unique trainer names, then uploading with generate their ids

unique_trainers = academy_csv_df[['trainers_first_name', 'trainers_last_name']].drop_duplicates()
trainers_df = unique_trainers.rename(columns={'trainers_first_name':'trainer_firstname','trainers_last_name':'trainers_lastname'})

# courses table can be made by finding the unique course names, then uploading to SQL with generate their ids

unique_courses = json_df['course_interest'].drop_duplicates()
courses_df = unique_courses.rename(columns={'course_interest': 'course_name'})


# Once all the dfs have been formed for each table, we can upload them to the DB

def load_to_sql(df, tablename):
    '''
    Function to load dataframe into SQL server table
    '''
    
    # Create SQLAlchemy engine to connect to SQL Server
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn_str))

    # Insert the dataframe into SQL Server
    df.to_sql(tablename, engine, if_exists='append', index=False)