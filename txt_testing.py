import unittest
import data_access


class Txt_Tests(unittest.TestCase):
    def test_df_size(self):
        """
        Checks that the data frame outputted by the function "write_to_dataframe" is the correct size
        (pre splitting name)
        """
        bucket = data_access.s3_connect('data-249-final-project')
        txt = data_access.read_txt(bucket)
        actual_size = data_access.write_to_dataframe(txt).size
        expected_size = 12402  # changes to 16536 when first and surname are split
        self.assertEqual(expected_size, actual_size, 'Error, size of DF should be 12402')

    def test_txt_output(self):
        """
        Checks the txt output is the right length and format
        """
        bucket = data_access.s3_connect('data-249-final-project')
        txt = data_access.read_txt(bucket)

        expected_length = 152  # taken from s3
        actual_length = len(txt)
        self.assertEqual(expected_length, actual_length, 'Error, not every txt file is merged correctly')

    def test_bucket(self):
        """
        Tests the bucket is retrieved and connects
        """
        expected_bucket = "s3.Bucket(name='data-249-final-project')"
        actual_bucket = str(data_access.s3_connect('data-249-final-project'))
        self.assertEqual(expected_bucket, actual_bucket, 'Error: buckets do not match')

    def test_columns(self):
        """
        Tests that the data frame has the correct number of columns with correct titles (pre name split)
        """
        bucket = data_access.s3_connect('data-249-final-project')
        txt = data_access.read_txt(bucket)
        df = data_access.write_to_dataframe(txt)
        actual_columns = list(df.columns)
        expected_columns = ['Name', 'Psychometric Score', 'Presentation Score']
        self.assertEqual(actual_columns, expected_columns, 'Error: Columns do not match')


if __name__ == '__main__':
    unittest.main()
