import os
import pandas as pd
import subprocess
import datetime
import boto3
 
# Localstack endpoint URL
S3_ENDPOINT_URL = 'http://localhost:4566'
 
# Define the S3 bucket and file paths
bucket_name = 'nyc-duration'
input_key = 'in/2023-01.parquet'
input_file = f"s3://{bucket_name}/{input_key}"
predictions_key = 'out/2023-01-predictions.parquet'
predictions_file = f"s3://{bucket_name}/{predictions_key}"
 
# Set up storage options for Localstack
options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
}
 
def create_bucket_if_not_exists(bucket_name):
    try:
        s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT_URL)
        buckets = s3.list_buckets()['Buckets']
        bucket_names = [bucket['Name'] for bucket in buckets]
        if bucket_name not in bucket_names:
            s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created")
        else:
            print(f"Bucket '{bucket_name}' already exists")
    except Exception as e:
        print(f"Error creating bucket '{bucket_name}': {e}")
 
def create_and_save_dataframe():
    try:
        # Create a sample DataFrame with fixed dates for consistency
        df_input = pd.DataFrame({
            'pickup_datetime': [
                '2023-01-01 00:00:00', 
                '2023-01-01 00:10:00', 
                '2023-01-01 00:20:00'
            ],
            'dropOff_datetime': [
                '2023-01-01 00:10:00', 
                '2023-01-01 00:20:00', 
                '2023-01-01 00:30:00'
            ],
            'PUlocationID': [1, 2, 3],
            'DOlocationID': [4, 5, 6]
        })
        # Convert datetime columns to datetime dtype
        df_input['pickup_datetime'] = pd.to_datetime(df_input['pickup_datetime'])
        df_input['dropOff_datetime'] = pd.to_datetime(df_input['dropOff_datetime'])
        # Save DataFrame to Parquet format in S3
        df_input.to_parquet(
            input_file,
            engine='pyarrow',
            compression=None,
            index=False,
            storage_options=options
        )
        print(f"Dataframe saved to {input_file}")
    except Exception as e:
        print(f"Error creating and saving dataframe: {e}")
 
def read_data(file_path, options=None):
    try:
        if options is None:
            options = {
                'storage_options': {
                    'client_kwargs': {
                        'endpoint_url': S3_ENDPOINT_URL
                    }
                }
            }
        # Check if S3_ENDPOINT_URL is set and use it for reading
        if S3_ENDPOINT_URL:
            options['storage_options']['client_kwargs']['endpoint_url'] = S3_ENDPOINT_URL
 
        df = pd.read_parquet(file_path, **options)
        return df
    except Exception as e:
        print(f"Error reading data from {file_path}: {e}")
        return None
 
def run_batch_script(input_file, predictions_file, ride_id):
    try:
        # Run batch.py script for January 2023 with specific ride_id
        batch_command = f"python batch.py {input_file} {predictions_file} --ride_id {ride_id}"
        subprocess.run(batch_command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running batch script: {e}")
    except Exception as e:
        print(f"Unexpected error running batch script: {e}")
 
def main(month, year, ride_id):
    try:
        categorical = ['PUlocationID', 'DOlocationID']
        month_num = datetime.datetime.strptime(month, '%b').month
        s3_bucket = 'nyc-duration'
        input_key = f'in/{year:04d}-{month_num:02d}.parquet'
        input_file = f"s3://{s3_bucket}/{input_key}"
        output_key = f'out/{year:04d}-{month_num:02d}-predictions.parquet'
        output_file = f"s3://{s3_bucket}/{output_key}"
        # Ensure the S3 bucket exists
        create_bucket_if_not_exists(s3_bucket)
        # Create and save the dataframe to S3
        create_and_save_dataframe()
        # Run the batch processing script for one ride_id
        run_batch_script(input_file, output_file, ride_id)
        # Read the predictions for one ride_id and calculate the sum
        df_result = read_data(output_file)
        if df_result is not None:
            print('DataFrame read from predictions file:')
            print(df_result.head())  # Print the first few rows for inspection
            sum_predicted_duration = df_result[df_result['ride_id'] == ride_id]['predicted_duration'].sum()
            print(f'Sum of predicted durations for ride_id {ride_id}:', sum_predicted_duration)
        else:
            print('No data read from predictions file.')
 
    except Exception as e:
        print(f"Unexpected error in main function: {e}")
 
if __name__ == "__main__":
    ride_id = 0  
    main("jan", 2023, ride_id)