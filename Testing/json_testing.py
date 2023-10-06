import unittest
from unittest.mock import Mock, patch
from io import BytesIO
import json
import pandas as pd
from data_access import json_df  # Import json_df function from main script


class TestJsonToDataFrame(unittest.TestCase):
    def setUp(self):
        # Mock S3 client and resource
        self.mock_s3_client = Mock()
        self.mock_s3_resource = Mock()
        self.mock_boto3_client = patch('boto3.client', return_value=self.mock_s3_client)
        self.mock_boto3_resource = patch('boto3.resource', return_value=self.mock_s3_resource)

        self.mock_boto3_client.start()
        self.mock_boto3_resource.start()

        # Mock S3 response with JSON data
        self.bucket_name = 'data-249-final-project'
        self.directory_prefix = 'Talent/'
        self.mock_s3_response = {
            'Contents': [
                {
                    'Key': f'{self.directory_prefix}mock.json'
                }
            ]
        }

        # Mock JSON data with 10 columns and 1 row
        self.mock_json_data = {
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

        self.mock_s3_client.list_objects.return_value = self.mock_s3_response

        # Mock S3.Object().get() and read() methods
        self.mock_s3_object = Mock()
        self.mock_s3_resource.Object.return_value = self.mock_s3_object
        self.mock_s3_object.get.return_value = {'Body': BytesIO(json.dumps(self.mock_json_data).encode())}

        # Call json_df function, store result
        self.df_array = json_df()

    def tearDown(self):
        self.mock_boto3_resource.stop()
        self.mock_boto3_client.stop()

    def test_check_df_rows_and_columns(self):
        # Check rows and columns in df_array
        for idx, df in enumerate(self.df_array):
            self.assertEqual(df.shape, (1, 10))  # Check expected shape (1 row, 10 columns - EDIT FOR SPLIT NAMES)

    def test_check_df_not_none(self):
        # Check if DF is okay in df_array
        for idx, df in enumerate(self.df_array):
            self.assertIsInstance(df, pd.DataFrame)  # Check if it's a DF not NONE.


if __name__ == "__main__":
    unittest.main()
