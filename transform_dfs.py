import transformations as tf
from main import *

talent_df = main()


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
custom_degree_order = {'1st': 1, '2:1': 2, '2:2': 3, '3rd': 4}
degree_df = create_csv_df(talent_df, 'degree', custom_degree_order)
city_df = create_csv_df(talent_df, 'city')


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


transform_csv_talent(talent_df)

talent_df = talent_df.reset_index(drop=True)
talent_df['talent_id'] = talent_df.index + 1
col = talent_df.columns
# print(col)
desired_order = ['talent_id', 'first_name', 'last_name', 'gender', 'dob', 'email', 'city', 'house_number',
                 'street_name', 'postcode', 'phone_number', 'uni', 'degree', 'invited_date', 'month',
                 'invited_first_name', 'invited_last_name']
talent_df = talent_df[desired_order]

#print(talent_df)

# Merged df to replace uni degree and city with corresponding primary keys
merged_df = talent_df.merge(uni_df, how='left', on='uni')
merged_df = merged_df.merge(degree_df, how='left', on='degree')
merged_df = merged_df.merge(city_df, how='left', on='city')

# Drop the uni, degree, and city columns from the merged DataFrame
merged_df = merged_df.drop(columns=['uni', 'degree', 'city'])

# Print the merged DataFrame
print(merged_df)

# Creating the address junction table
columns_to_extract = ['talent_id', 'city_id', 'house_number', 'street_name', 'postcode']
address_junction_df = merged_df[columns_to_extract]
print(address_junction_df)

# Creating the uni junction table
columns_to_extract = ['talent_id', 'uni_id', 'degree_id']
uni_junction_df = merged_df[columns_to_extract]
print(uni_junction_df)