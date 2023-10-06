from transform_json import *
import data_access
import pandas as pd

bucket = data_access.resource_connection('data-249-final-project')


def extract_strengths_table(df):
    # Store distinct strengths
    distinct_strengths = []
    strength_columns = ['strengths 1', 'strengths 2', 'strengths 3']
    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        for strength_col in strength_columns:
            strength = row[strength_col]
            if not pd.isnull(strength) and strength not in distinct_strengths:
                distinct_strengths.append(strength)

    # Create a DataFrame from the list of distinct strengths with a custom index
    extracted_strengths_df = pd.DataFrame({'Strength': distinct_strengths})

    # Add Custom index
    extracted_strengths_df['strengths_ID'] = extracted_strengths_df.index + 1

    return extracted_strengths_df


def extract_weakness_table(df):
    # Store distinct strengths
    distinct_weakness = []
    weakness_columns = ['weaknesses 1', 'weaknesses 2', 'weaknesses 3']
    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        for weakness_col in weakness_columns:
            weakness = row[weakness_col]
            if not pd.isnull(weakness) and weakness not in distinct_weakness:
                distinct_weakness.append(weakness)

    # Create a DataFrame from the list of distinct strengths with a custom index
    extracted_weakness_df = pd.DataFrame({'Weakness': distinct_weakness})

    # Add custom index
    extracted_weakness_df['weakness_id'] = extracted_weakness_df.index + 1

    return extracted_weakness_df


def recruiter_id_df(df_convert):
    df = pd.concat([df_convert['invited_first_name'], df_convert['invited_last_name']], axis=1, keys=['invited_first_name', 'invited_last_name'])
    df = df[['invited_first_name', 'invited_last_name']].drop_duplicates()
    df = df.dropna().reset_index(drop=True)
    df['recruiter_id'] = df.index + 1
    df = df[['recruiter_id', 'invited_first_name', 'invited_last_name']]
    return df


def tech_score_table():
    tech_score_columns = ['C#', 'C++', 'Java', 'Python', 'R', 'JavaScript', 'Ruby', 'SPSS', 'PHP']
    # Create a DataFrame from the list with a single column
    extracted_tech_df = pd.DataFrame({'Tech': tech_score_columns})

    # Create index starting from 1
    extracted_tech_df['tech_score_id'] = extracted_tech_df.index + 1

    return extracted_tech_df


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
    desired_order = ['index', column_name]
    # ordering the columns
    ordered_df = ordered_df[desired_order]

    return ordered_df


def create_behaviours_df(academy_df):
    behaviours = academy_df.columns[4:]
    behaviours = behaviours[:6]
    titles = []
    for title in behaviours:
        title = title.split('_')
        title = title[0]
        titles.append(title)
    behaviours_df = pd.DataFrame(titles, columns=['behaviours'])
    behaviours_df['behaviour_id'] = behaviours_df.index + 1
    behaviours_df = behaviours_df[['behaviour_id', 'behaviours']]
    return behaviours_df


def create_trainers_df(academy_df):
    trainer_df_first = academy_df['trainer_first_name']
    trainer_df_last = academy_df['trainer_last_name']
    trainer_df = pd.concat([trainer_df_first, trainer_df_last], axis=1, ignore_index=True)
    trainer_df.drop_duplicates(keep='first', inplace=True)

    trainer_id = []
    for x in range(len(trainer_df.index)):
        trainer_id.append(x + 1)
    trainer_id_df = pd.DataFrame(trainer_id, columns=['trainer_id'])
    trainer_df = pd.concat([trainer_df.reset_index(drop=True), trainer_id_df.reset_index(drop=True)], axis=1)
    trainer_df = trainer_df[['trainer_id', 0, 1]]
    trainer_df.rename(columns={0: 'trainer_first_name', 1: 'trainer_last_name'}, inplace=True)
    return trainer_df


def courses_table(df):
    course_column = ['course_interest']
    distinct_courses = []
    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        for course_col in course_column:
            course = row[course_col]
            if not pd.isnull(course) and course not in distinct_courses:
                distinct_courses.append(course)

    # Create a DataFrame from the list of distinct strengths with a custom index
    extracted_courses_df = pd.DataFrame({'course': distinct_courses})

    # Add custom index
    extracted_courses_df['course_id'] = extracted_courses_df.index + 1

    return extracted_courses_df


if __name__ == "__main__":
    example_dfs = json_df()
    processed_df = process_example_dfs(example_dfs)

    distinct_strengths_df = extract_strengths_table(processed_df)
    distinct_weaknesses_df = extract_weakness_table(processed_df)
    tech_score_df = techscore_table()
    print(distinct_strengths_df)
    print(distinct_weaknesses_df)
    print(techscore_df)