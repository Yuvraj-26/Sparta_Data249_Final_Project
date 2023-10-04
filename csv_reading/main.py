from csv_to_df import CsvToDf
import data_access

if __name__ == "__main__":
    bucket_name = 'data-249-final-project'
    prefix = 'Academy'
    data_loader = CsvToDf(bucket_name, prefix)
    data_loader.fetch_all_data()
    talent_df = data_loader.load_dataframes()

print(talent_df)

# bucket = data_access.s3_connect('data-249-final-project')
# file = data_access.read_csv(bucket, 'Academy')
# print(file)
