from tables import *


def replace_weakness_with_id(main_df, weakness_df):

    weakness_mapping = dict(zip(weakness_df['Weakness'], weakness_df['weakness_id']))

    # Columns to replace with ID
    weakness_columns = ['weaknesses 1', 'weaknesses 2', 'weaknesses 3']

    # Iterate and replace weaknesses with IDs
    for col in weakness_columns:
        main_df[col] = main_df[col].map(weakness_mapping)

    return main_df


def replace_strength_with_id(main_df, strength_df):
    strength_mapping = dict(zip(strength_df['Strength'], strength_df['strengths_ID']))

    # Columns to replace with ID
    strength_columns = ['strengths 1', 'strengths 2', 'strengths 3']

    # Iterate and replace weaknesses with IDs
    for col in strength_columns:
        main_df[col] = main_df[col].map(strength_mapping)

    return main_df

def replace_techscore(main_df, tech_df):
    tech_mapping = dict(zip(tech_df['tech_score'], tech_df['tech_score_id']))

    # Columns to replace with ID
    tech_score_columns = ['C#', 'C++', 'Java', 'Python', 'R', 'JavaScript', 'Ruby', 'SPSS', 'PHP']

    # Iterate and replace tech with IDs
    for col in tech_score_columns:
        main_df[col] = main_df[col].map(tech_mapping)

    return main_df

if __name__ == "__main__":#
    example_dfs = json_df()
    processed_df = process_example_dfs(example_dfs)

    distinct_strengths_df = extract_strengths_table(processed_df)
    distinct_weaknesses_df = extract_weakness_table(processed_df)
    distinct_courses_df = courses_table(processed_df)
    techscore_df = techscore_table()

    merged_weakness = replace_weakness_with_id(processed_df, distinct_weaknesses_df)
    merged_strengths = replace_strength_with_id(processed_df, distinct_strengths_df)

    merged_techscore = replace_techscore(processed_df, techscore_df)

    print(merged_weakness)
    print(merged_strengths)
    print(merged_techscore)