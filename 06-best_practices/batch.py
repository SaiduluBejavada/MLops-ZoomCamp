# #!/usr/bin/env python
# # coding: utf-8

# import sys
# import os
# import pickle
# import pandas as pd

# # Set environment variables for LocalStack
# os.environ['AWS_ACCESS_KEY_ID'] = 'test123'
# os.environ['AWS_SECRET_ACCESS_KEY'] = 'test123'
# os.environ['S3_ENDPOINT_URL'] = 'http://localhost:4566'

# input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
# output_file = f'output/yellow_tripdata_{year:04d}-{month:02d}.parquet'


# with open('model.bin', 'rb') as f_in:
#     dv, lr = pickle.load(f_in)

# def prepare_data(df, categorical):
#     df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
#     df['duration'] = df.duration.dt.total_seconds() / 60

#     df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

#     df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

#     return df


# def read_data(filename, categorical):
#     S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
#     print(f"S3_ENDPOINT_URL: {S3_ENDPOINT_URL}")

#     if S3_ENDPOINT_URL is not None:
#         options = {
#             'client_kwargs': {
#                 'endpoint_url': S3_ENDPOINT_URL
#             }
#         }

#         df = pd.read_parquet(filename, storage_options=options)
#     else:
#         df = pd.read_parquet(filename)

#     return prepare_data(df, categorical)


# def save_data(filename, df):
#     S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
#     print(f"S3_ENDPOINT_URL: {S3_ENDPOINT_URL}")

#     if S3_ENDPOINT_URL is not None:
#         options = {
#             'client_kwargs': {
#                 'endpoint_url': "http://localhost:4566"
#             }
#         }
#         # print(f"Reading data from {filename} with options: {options}")
#         # try:
#         #     df = pd.read_parquet(filename, storage_options=options)
#         #     print("Data read successfully")
#         # except Exception as e:
#         #     print(f"Error reading data: {e}")
#         # return df

#         df.to_parquet(filename, engine='pyarrow', index=False, storage_options=options)
#     else:
#         df.to_parquet(filename, engine='pyarrow', index=False)


# # def get_input_path(year, month):
# #     default_input_pattern = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
# #     input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
# #     return input_pattern.format(year=year, month=month)

# def get_input_path(year, month):
#     default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
#     input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
#     print(input_pattern)
#     print(input_pattern.format(year=year, month=month))
#     return input_pattern.format(year=year, month=month)

# def get_output_path(year, month):
#     default_output_pattern = f's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
#     output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
#     return output_pattern.format(year=year, month=month)


# def main(year, month):
#     input_file = get_input_path(year, month)
#     output_file = get_output_path(year, month)

#     categorical = ['PULocationID', 'DOLocationID']

#     df = read_data(input_file, categorical)
#     df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

#     with open('model.bin', 'rb') as f_in:
#         dv, lr = pickle.load(f_in)

#     dicts = df[categorical].to_dict(orient='records')
#     X_val = dv.transform(dicts)
#     y_pred = lr.predict(X_val)

#     print('predicted mean duration:', y_pred.mean())

#     df_result = pd.DataFrame()
#     df_result['ride_id'] = df['ride_id']
#     df_result['predicted_duration'] = y_pred

#     save_data(output_file, df_result)


# if __name__ == '__main__':
#     year = int(sys.argv[1])
#     month = int(sys.argv[2])

#     main(year=year, month=month)


#!/usr/bin/env python
# coding: utf-8

# import os
# import sys
# import pickle
# import pandas as pd

# # Set environment variables for LocalStack
# os.environ['AWS_ACCESS_KEY_ID'] = 'test123'
# os.environ['AWS_SECRET_ACCESS_KEY'] = 'test123'
# os.environ['S3_ENDPOINT_URL'] = 'http://localhost:4566'

# def prepare_data(df, categorical):
#     df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
#     df['duration'] = df.duration.dt.total_seconds() / 60

#     df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

#     df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

#     return df

# def read_data(filename, categorical):
#     S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
#     print(f"S3_ENDPOINT_URL: {S3_ENDPOINT_URL}")

#     options = None
#     if S3_ENDPOINT_URL:
#         options = {
#             'client_kwargs': {
#                 'endpoint_url': S3_ENDPOINT_URL
#             }
#         }

#     df = pd.read_parquet(filename, storage_options=options)
#     return prepare_data(df, categorical)

# def save_data(filename, df):
#     S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
#     print(f"S3_ENDPOINT_URL: {S3_ENDPOINT_URL}")

#     options = None
#     if S3_ENDPOINT_URL:
#         options = {
#             'client_kwargs': {
#                 'endpoint_url': S3_ENDPOINT_URL
#             }
#         }

#     df.to_parquet(filename, engine='pyarrow', index=False, storage_options=options)

# def get_input_path(year, month):
#     default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
#     input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
#     return input_pattern.format(year=year, month=month)

# def get_output_path(year, month):
#     default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
#     output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
#     return output_pattern.format(year=year, month=month)

# def main(year, month):
#     input_file = get_input_path(year, month)
#     output_file = get_output_path(year, month)

#     categorical = ['PULocationID', 'DOLocationID']

#     df = read_data(input_file, categorical)
#     df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

#     with open('model.bin', 'rb') as f_in:
#         dv, lr = pickle.load(f_in)

#     dicts = df[categorical].to_dict(orient='records')
#     X_val = dv.transform(dicts)
#     y_pred = lr.predict(X_val)

#     print('predicted mean duration:', y_pred.mean())

#     df_result = pd.DataFrame()
#     df_result['ride_id'] = df['ride_id']
#     df_result['predicted_duration'] = y_pred

#     save_data(output_file, df_result)

# if __name__ == '__main__':
#     year = int(sys.argv[1])
#     month = int(sys.argv[2])

#     main(year=year, month=month)


import os
import sys
import pickle
import pandas as pd

# Set environment variables for LocalStack
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['S3_ENDPOINT_URL'] = 'http://localhost:4566'

def prepare_data(df, categorical):
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df

def read_data(filename, categorical):
    print(f"Reading data from: {filename}")
    df = pd.read_parquet(filename)
    return prepare_data(df, categorical)

def save_data(filename, df):
    S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
    print(f"S3_ENDPOINT_URL: {S3_ENDPOINT_URL}")
    print(f"Saving data to: {filename}")

    if S3_ENDPOINT_URL is not None:
        options = {
            'client_kwargs': {
                'endpoint_url': str(S3_ENDPOINT_URL)
            }
        }
        df.to_parquet(filename, engine='pyarrow', index=False, storage_options=options)
    else:
        df.to_parquet(filename, engine='pyarrow', index=False)

def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)

def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)

def main(year, month):
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")

    categorical = ['PULocationID', 'DOLocationID']

    df = read_data(input_file, categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print('Predicted mean duration:', y_pred.mean())

    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    save_data(output_file, df_result)

if __name__ == '__main__':
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year=year, month=month)
