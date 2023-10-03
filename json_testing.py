import unittest
from unittest.mock import Mock, patch
from io import BytesIO
import json
import pandas as pd
from Extracting_JSON import json_df  # Import json_df function from main script

class TestJsonToDataFrame(unittest.TestCase):
    @patch('boto3.client')
    @patch('boto3.resource')
    def test_json_extraction(self, mock_boto3_resource, mock_boto3_client):
        # Mock S3 client and resource
        mock_s3_client = Mock()
        mock_s3_resource = Mock()
        mock_boto3_client.return_value = mock_s3_client
        mock_boto3_resource.return_value = mock_s3_resource

        # Mock S3 response with JSON data
        bucket_name = 'data-249-final-project'
        directory_prefix = 'Talent/'
        mock_s3_response = {
            'Contents': [
                {
                    'Key': f'{directory_prefix}mock.json'
                }
            ]
        }

        # Mock JSON data with 10 columns and 1 row
        mock_json_data = {
            'column1': 'value1',
            'column2': 'value2',
            'column3': 'value3',
            'column4': 'value4',
            'column5': 'value5',
            'column6': 'value6',
            'column7': 'value7',
            'column8': 'value8',
            'column9': 'value9',
            'column10': 'value10'
        }

        mock_s3_client.list_objects.return_value = mock_s3_response

        # Mock S3.Object().get() and read() methods
        mock_s3_object = Mock()
        mock_s3_resource.Object.return_value = mock_s3_object
        mock_s3_object.get.return_value = {'Body': BytesIO(json.dumps(mock_json_data).encode())}

        # Call json_df function, store result
        df_array = json_df()

        # Add assertions to check correctness of df_array
        self.check_df_rows_and_columns(df_array)
        self.check_df_not_none(df_array)

    def check_df_rows_and_columns(self, df_array):
        # Check rows and columns in df_array
        for idx, df in enumerate(df_array):
            self.assertEqual(df.shape, (1, 10))  # Check expected shape (1 row, 10 columns - EDIT FOR SPLIT NAMES)

    def check_df_not_none(self, df_array):
        # Check if DF is okay in df_array
        for idx, df in enumerate(df_array):
            self.assertIsInstance(df, pd.DataFrame)  # Check if it's a DF not NONE

if __name__ == "__main__":
    unittest.main()
