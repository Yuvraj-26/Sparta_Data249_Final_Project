import boto3
import pandas as pd


class CsvToDf:

    def __init__(self, bucket_name, prefix):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.all_objects = []

    def _list_objects(self, continuation_token=None):
        if continuation_token:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=self.prefix, ContinuationToken=continuation_token)
        else:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=self.prefix)
        return response

    def _get_data_from_s3(self, key):
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        df = pd.read_csv(response['Body'])
        return df

    def fetch_all_data(self):
        continuation_token = None
        while True:
            response = self._list_objects(continuation_token)
            self.all_objects.extend(response.get('Contents', []))
            if response.get('IsTruncated'):
                continuation_token = response['NextContinuationToken']
            else:
                break

    def load_dataframes(self):
        all_data = []
        for obj in self.all_objects:
            if obj['Key'].endswith('.csv'):
                df = self._get_data_from_s3(obj['Key'])
                all_data.append(df)
        return pd.concat(all_data, ignore_index=True)


