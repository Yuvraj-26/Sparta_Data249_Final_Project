import transformations as tf

def transform_txt(df):
    tf.split_column_on_space(df, 'name', 'first_name', 'last_name')
    tf.convert_to_int(df, 'psychometric_score')
    tf.convert_to_int(df, 'presentation_score')
    return df

def transform_csv_talent(df):
    tf.replace_strings_in_column(df, 'invited_by', "Bruno Belbrook", "Bruno Bellbrook")
    tf.replace_strings_in_column(df, 'invited_by', "Fifi Etton", "Fifi Eton")
    tf.split_column_on_space(df, 'name', 'first_name', 'last_name')
    tf.convert_rows_to_lower(df, 'first_name')
    tf.convert_rows_to_lower(df, 'last_name')
    tf.reverse_date(df, 'dob')
    df.drop(columns='id', inplace=True)
    tf.convert_column_to_date(df, 'dob')
    tf.split_column_on_space(df, 'invited_by', 'invited_first_name', 'invited_last_name')
    tf.split_column_on_space(df, 'address', 'house_number', 'street_name')
    tf.replace_strings_in_column(df, 'month', 'SEPT 2019', 'September 2019')
    tf.convert_nan_to_0(df, 'invited_date')
    tf.convert_to_int(df, 'invited_date')
    tf.convert_rows_to_string(df, 'invited_date')
    tf.merge_columns(df, 'invited_date', 'month')
    df.drop(columns='month')
    tf.convert_column_to_date_full(df, 'invited_date')
    tf.apply_standard_uk_number_to_column(df, 'phone_number')
    return df

def transform_academy_df(dataframe):
    tf.lower_case_columns(dataframe)
    tf.convert_rows_to_lower(dataframe,'name')
    tf.convert_rows_to_lower(dataframe, 'trainer')
    tf.split_column_on_space(dataframe, 'trainer', 'trainer_first_name', 'trainer_last_name')
    tf.split_column_on_space(dataframe,'name', 'first_name', 'last_name')
    tf.convert_all_nan_to_0(dataframe)
    for col in dataframe.columns[0]:
        try:
            tf.convert_to_int(dataframe,col)
        except:
            pass
    tf.convert_all_0_to_nan(dataframe)
    return dataframe