from csv_to_df import CsvToDf

if __name__ == "__main__":
    bucket_name = 'data-249-final-project'
    prefix = 'Talent'
    data_loader = CsvToDf(bucket_name, prefix)
    data_loader.fetch_all_data()
    talent_df = data_loader.load_dataframes()

print(talent_df)

