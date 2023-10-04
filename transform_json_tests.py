import pandas as pd
import pytest
from transform_json import *

# Create sample data for testing
sample_data = {
    'tech_self_score': [{'Python': 5, 'JavaScript': 4}, {'Python': 3, 'Java': 4}],
    'strengths': [['Communication', 'Problem Solving'], ['Time Management']],
    'weaknesses': [['Time Management', None], ['Communication', 'Time Management']],
    'name': ['John Doe', 'Jane Smith'],
    'date': ['01/05/2023', '15/02/2022'],
    'self_development': ['Yes', 'No'],
    'geo_flex': ['No', 'Yes'],
    'financial_support_self': ['Yes', 'No']
}

sample_df = pd.DataFrame(sample_data)

class TestDataCleaningFunctions:
    def test_expand_tech_self_score(self):
        expanded_df = expand_tech_self_score(sample_df)
        expected_columns = ['Python', 'JavaScript', 'Java']
        assert all(col in expanded_df.columns for col in expected_columns)

    def test_expand_strength_weakness(self):
        expanded_df = expand_strength_weakness(sample_df)
        expected_strength_columns = ['strengths 1', 'strengths 2', ]
        expected_weakness_columns = ['weaknesses 1', 'weaknesses 2', ]
        assert all(col in expanded_df.columns for col in expected_strength_columns)
        assert all(col in expanded_df.columns for col in expected_weakness_columns)

    def test_split_name_column(self):
        split_df = split_name_column(sample_df)
        assert 'first_name' in split_df.columns
        assert 'last_name' in split_df.columns

    def test_convert_yes_no_to_binary(self):
        converted_df = convert_yes_no_to_binary(sample_df)
        assert all(val in [0, 1] for val in converted_df['self_development'])

    def test_convert_date(self):
        converted_df = convert_date(sample_df)
        assert pd.api.types.is_datetime64_any_dtype(converted_df['date'])


if __name__ == '__main__':
    pytest.main()
