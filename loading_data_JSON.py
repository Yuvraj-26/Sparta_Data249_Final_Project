import transformations
from main import *
from data_access import *
import transform_json

json_df = data_access.json_df()

json_df = transform_json.process_example_dfs(json_df)

json_df = transformations.convert_rows_to_lower(json_df, 'first_name')
json_df = transformations.convert_rows_to_lower(json_df, 'last_name')

