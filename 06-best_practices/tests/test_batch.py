import pytest
import pandas as pd
from datetime import datetime

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(1, 30, 0)),  
]

columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df = pd.DataFrame(data, columns=columns)

def prepare_data(df):
    df = df.copy()
    # Drop rows with None values in 'PULocationID' or 'DOLocationID'
    df = df.dropna(subset=['PULocationID', 'DOLocationID'], how='any')
    # Calculate duration in minutes
    df['duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    # Filter based on duration (1 to 60 minutes)
    df = df[(df['duration'] >= 1) & (df['duration'] <= 60)]
    # Convert location IDs to integer (if necessary)
    df['PULocationID'] = df['PULocationID'].astype(int)
    df['DOLocationID'] = df['DOLocationID'].astype(int)
    # Reset index to ensure it starts from 0 and is sequential
    df = df.reset_index(drop=True)
    return df

df_processed = prepare_data(df)

# Define expected output including 'duration'
expected_data = [
    (1, 1, dt(1, 2), dt(1, 10), 8.0),
    (3, 4, dt(1, 2, 0), dt(1, 30, 0), 28.0)
]
expected_columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration']
expected_df = pd.DataFrame(expected_data, columns=expected_columns)

# Print the first few rows of both dataframes
print("Processed Dataframe:")
print(df_processed.head())

print("\nExpected Dataframe:")
print(expected_df.head())

def test_prepare_data():
    # Define input data
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(1, 30, 0)),
    ]
    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df = pd.DataFrame(data, columns=columns)

    # Define expected output including 'duration'
    expected_data = [
        (1, 1, dt(1, 2), dt(1, 10), 8.0),
        (3, 4, dt(1, 2, 0), dt(1, 30, 0), 28.0)
    ]
    expected_columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration']
    expected_df = pd.DataFrame(expected_data, columns=expected_columns)

    df_processed = prepare_data(df)
    pd.testing.assert_frame_equal(df_processed, expected_df)

if __name__ == "__main__":
    test_prepare_data()
    print("All tests passed.")
