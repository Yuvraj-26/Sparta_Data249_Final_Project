
from transform_json import *

import pandas as pd

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

def techscore_table():
    tech_score_columns = ['C#', 'C++', 'Java', 'Python', 'R', 'JavaScript', 'Ruby', 'SPSS', 'PHP']
    # Create a DataFrame from the list with a single column
    extracted_tech_df = pd.DataFrame({'tech_score': tech_score_columns})

    # Create index starting from 1
    extracted_tech_df['tech_score_id'] = extracted_tech_df.index + 1

    return extracted_tech_df

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
    distinct_courses_df = courses_table(processed_df)
    techscore_df = techscore_table()
    print(distinct_strengths_df)
    print(distinct_weaknesses_df)
    print(techscore_df)
    print(distinct_courses_df)
