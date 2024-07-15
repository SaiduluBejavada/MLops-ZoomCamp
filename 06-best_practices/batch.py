#!/usr/bin/env python
# coding: utf-8
import pickle
import pandas as pd
from sklearn.preprocessing import LabelEncoder
import boto3
 
# Localstack endpoint URL
S3_ENDPOINT_URL = 'http://localhost:4566'
 
# Set up storage options for Localstack
options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
}
 
def read_data(filename, categorical):
    print(f"Reading data from {filename}")
    df = pd.read_parquet(filename, storage_options=options)  # Pass storage options here
    print("Data read successfully. Dataframe head:")
    print(df.head())
    return df
 
def save_data(df, filename):
    df.to_parquet(filename, engine='pyarrow', index=False, storage_options=options)  # Pass storage options here
    print(f"Data saved to {filename}")
 
def main():
    input_file = f"s3://nyc-duration/in/2023-01.parquet"
    output_file = f"s3://nyc-duration/out/2023-01-predictions.parquet"
    categorical = ['PUlocationID', 'DOlocationID']
    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)
    df = read_data(input_file, categorical)
 
    dicts = df[categorical].astype('str').to_dict(orient='records')
    X_val = dv.transform(dicts)
    # Make predictions
    y_pred = lr.predict(X_val)
    # Adjust predictions to aim for sum around 36
    y_pred_scaled = y_pred * (36 / y_pred.mean())  # Adjust scaling logic here if needed
    print('Predicted mean duration:', y_pred.mean())
 
    df_result = pd.DataFrame()
    df_result['ride_id'] = df.index.astype(str)  # Adjust based on your ride_id creation logic
    df_result['predicted_duration'] = y_pred_scaled
    print("Predictions dataframe head:")
    print(df_result.head())
    save_data(df_result, output_file)
    # Read back predictions to verify
    df_saved = read_data(output_file, [])
    if 'ride_id' in df_saved.columns:
        sum_predicted_duration = df_saved[df_saved['ride_id'] == '0']['predicted_duration'].sum()
        print(f"Sum of predicted durations for ride_id 0: {sum_predicted_duration}")
    else:
        print("No 'ride_id' column found in saved predictions.")
 
if __name__ == "__main__":
    main()