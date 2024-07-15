# # import os
# # import pandas as pd
# # from datetime import datetime

# # import batch

# # # Set environment variables for LocalStack
# # os.environ['AWS_ACCESS_KEY_ID'] = 'test'
# # os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
# # os.environ['S3_ENDPOINT_URL'] = 'http://localhost:4566'


# # def dt(hour, minute, second=0):
# #     return datetime(2022, 1, 1, hour, minute, second)

# # S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')

# # # Define storage options only if S3_ENDPOINT_URL is set
# # options = {}
# # if S3_ENDPOINT_URL:
# #     options = {
# #         'client_kwargs': {
# #             'endpoint_url': S3_ENDPOINT_URL
# #         }
# #     }

# # data = [
# #     (None, None, dt(1, 2), dt(1, 10)),
# #     (1, None, dt(1, 2), dt(1, 10)),
# #     (1, 2, dt(2, 2), dt(2, 3)),
# #     (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
# #     (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
# #     (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
# # ]

# # columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
# # df_input = pd.DataFrame(data, columns=columns)

# # input_file = batch.get_input_path(2022, 1)
# # output_file = batch.get_output_path(2022, 1)

# # # Write input data to Parquet
# # df_input.to_parquet(
# #     input_file,
# #     engine='pyarrow',
# #     compression=None,
# #     index=False,
# #     storage_options=options if S3_ENDPOINT_URL else None  # Only pass options if S3_ENDPOINT_URL is set
# # )

# # # Run batch process
# # os.system(f'python batch.py 2022 1')

# # # Read output data from Parquet
# # df_actual = pd.read_parquet(output_file, engine='pyarrow', storage_options=options if S3_ENDPOINT_URL else None)

# # # Print and verify the sum of predicted durations
# # print(df_actual['predicted_duration'].sum())

# # # Assertion for expected sum of predicted durations
# # assert abs(df_actual['predicted_duration'].sum() - 31.51) < 0.1

# import os
# import pandas as pd
# from datetime import datetime

# import batch

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

# # Write input data to Parquet without storage_options for local operations
# df_input.to_parquet(
#     input_file,
#     engine='pyarrow',
#     compression=None,
#     index=False
# )

# # Run batch process
# os.system(f'python batch.py 2022 1')

# # Read output data from Parquet with storage_options for remote operations
# df_actual = pd.read_parquet(output_file, engine='pyarrow', storage_options=options if S3_ENDPOINT_URL else None)

# # Print and verify the sum of predicted durations
# print('-------sum of predicted durations----------')
# print(df_actual['predicted_duration'].sum())

# # Assertion for expected sum of predicted durations
# assert abs(df_actual['predicted_duration'].sum() - 31.51) < 0.1

import os
import pandas as pd
import subprocess
 
# Localstack endpoint URL
S3_ENDPOINT_URL = 'http://localhost:4566'
 
# Define the S3 bucket and file path
bucket_name = 'nyc-duration'
key = 'in/2023-01.parquet'
input_file = f"s3://{bucket_name}/{key}"
 
# Set up storage options for Localstack
options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
}
 
def create_and_save_dataframe():
    # Create a sample DataFrame (assuming you have it from Q3)
    df_input = pd.DataFrame({
        'A': [1, 2, 3],
        'B': [4, 5, 6]
    })
    # Save DataFrame to Parquet format in S3
    df_input.to_parquet(
        input_file,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
    )
 
def verify_file_creation_and_size():
    # Verify the file is created using AWS CLI
    aws_cli_command = f"aws --endpoint-url={S3_ENDPOINT_URL} s3 ls {input_file}"
    result = subprocess.run(aws_cli_command, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print("File was successfully created.")
        # Get the file size using AWS CLI --summarize option
        aws_cli_size_command = f"aws --endpoint-url={S3_ENDPOINT_URL} s3 ls {input_file} --summarize"
        size_result = subprocess.run(aws_cli_size_command, shell=True, capture_output=True, text=True)
        # Extract and print the total size of the file
        size_output_lines = size_result.stdout.splitlines()
        for line in size_output_lines:
            if line.startswith("Total Size"):
                file_size = int(line.split()[2])
                print(f"File size: {file_size} bytes")
                break
    else:
        print("Error: File creation failed.")
 
if __name__ == "__main__":
    create_and_save_dataframe()
    verify_file_creation_and_size()