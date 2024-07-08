# import os
# import pandas as pd
# from datetime import datetime

# import batch

# # Set environment variables for LocalStack
# os.environ['AWS_ACCESS_KEY_ID'] = 'test'
# os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
# os.environ['S3_ENDPOINT_URL'] = 'http://localhost:4566'


# def dt(hour, minute, second=0):
#     return datetime(2022, 1, 1, hour, minute, second)

# S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')

# # Define storage options only if S3_ENDPOINT_URL is set
# options = {}
# if S3_ENDPOINT_URL:
#     options = {
#         'client_kwargs': {
#             'endpoint_url': S3_ENDPOINT_URL
#         }
#     }

# data = [
#     (None, None, dt(1, 2), dt(1, 10)),
#     (1, None, dt(1, 2), dt(1, 10)),
#     (1, 2, dt(2, 2), dt(2, 3)),
#     (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
#     (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
#     (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
# ]

# columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
# df_input = pd.DataFrame(data, columns=columns)

# input_file = batch.get_input_path(2022, 1)
# output_file = batch.get_output_path(2022, 1)

# # Write input data to Parquet
# df_input.to_parquet(
#     input_file,
#     engine='pyarrow',
#     compression=None,
#     index=False,
#     storage_options=options if S3_ENDPOINT_URL else None  # Only pass options if S3_ENDPOINT_URL is set
# )

# # Run batch process
# os.system(f'python batch.py 2022 1')

# # Read output data from Parquet
# df_actual = pd.read_parquet(output_file, engine='pyarrow', storage_options=options if S3_ENDPOINT_URL else None)

# # Print and verify the sum of predicted durations
# print(df_actual['predicted_duration'].sum())

# # Assertion for expected sum of predicted durations
# assert abs(df_actual['predicted_duration'].sum() - 31.51) < 0.1

import os
import pandas as pd
from datetime import datetime

import batch

def dt(hour, minute, second=0):
    return datetime(2022, 1, 1, hour, minute, second)

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')

# Define storage options only if S3_ENDPOINT_URL is set
options = {}
if S3_ENDPOINT_URL:
    options = {
        'client_kwargs': {
            'endpoint_url': S3_ENDPOINT_URL
        }
    }

data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2), dt(1, 10)),
    (1, 2, dt(2, 2), dt(2, 3)),
    (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
]

columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df_input = pd.DataFrame(data, columns=columns)

input_file = batch.get_input_path(2022, 1)
output_file = batch.get_output_path(2022, 1)

# Write input data to Parquet without storage_options for local operations
df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False
)

# Run batch process
os.system(f'python batch.py 2022 1')

# Read output data from Parquet with storage_options for remote operations
df_actual = pd.read_parquet(output_file, engine='pyarrow', storage_options=options if S3_ENDPOINT_URL else None)

# Print and verify the sum of predicted durations
print('-------sum of predicted durations----------')
print(df_actual['predicted_duration'].sum())

# Assertion for expected sum of predicted durations
assert abs(df_actual['predicted_duration'].sum() - 31.51) < 0.1
