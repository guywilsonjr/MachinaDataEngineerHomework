import os
from typing import Any, Dict, Tuple

import pandas as pd
import dask.bag as db

from feature_engineering import add_engineered_features


sample_data_url = 'https://github.com/Machina-Labs/ml_ops_data_engineer_hw/raw/main/data/sample.parquet'

TIMEZONE = 'UTC'
# Here we assume the timezone is UTC.
column_dtypes = {
    'time': f'datetime64[ns, {TIMEZONE}]',
    'value': 'float64',
    'field': 'category',
    'robot_id': 'uint64',
    'run_uuid': 'uint64',
    'sensor_type': 'category',
}


def preprocess_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the dataframe. Here we convert the time column to a datetime object, and convert the other columns to
    the appropriate dtypes according to the following logic
    - time: datetime64[ns, UTC]
    - field: category
    - robot_id: int
    - run_uuid: int Run uuid are greater than 64 bits and must be converted to a python int as pandas does not support
      integers greater than 64 bits.
    - sensor_type: category
    :param df: Dataframe to preprocess
    :return: Preprocessed dataframe
    """

    df = df.astype(column_dtypes)
    return df



def convert_to_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the dataframe to a feature dataframe. Here we pivot the dataframe
    so that each column is a combination of the field from a robot_id and field entry
    :param df: Dataframe to convert
    :return: Feature dataframe
    """

    wide_df = df.pivot_table(index='time', columns=['field', 'robot_id'], values='value')

    # Flatten the `(field,robot_id)` MultiIndex columns to a single `field_robot_id` column
    wide_df.columns = ['_'.join(map(str, col)).strip() for col in wide_df.columns.values]

    # Reset index to make 'time' a column again
    return wide_df.sort_index().reset_index()



def match_timestamps_with_measurements(df: pd.DataFrame) -> pd.DataFrame:
    """
    Matches timestamps with measurements. Here we assume that the most recent measurement approximates the actual value
    of the measurement at the timestamp. We fill forward and then backward to fill in missing values.

    :param df: Dataframe to match timestamps with measurements
    :return: Dataframe with timestamps matched with measurements
    """

    df = df.ffill().bfill()
    return df


def get_dfs_by_run_uuid(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Splits the dataframe into a list of dataframes, one for each run_uuid.
    We assume that the data for each run_uuid is completely independent of data with a different run_uuid.

    :param df: Dataframe to split
    :return: List of dataframes
    """

    data = {}
    for run_uuid in df['run_uuid'].unique():
        data_df = df[df['run_uuid'] == run_uuid]
        data[run_uuid] = data_df
    return data


def serialize_run_data_to_file(run_uuid: int, df: pd.DataFrame) -> pd.DataFrame:
    df.to_csv(f'output/run_data_{run_uuid}.csv', index=False)
    return df


def calculate_run_stats(input_data: Tuple[int, pd.DataFrame]) -> Dict[str, Any]:
    """
    Calculates the run stats for a given run_uuid and it's associated DataFrame
    :param input_data: Tuple of run_uuid to calculate run stats for and Dataframe associated with the run_uuid
    :return: A dictionary containing the run stats
    """

    run_uuid, run_uuid_df = input_data
    run_start_time = run_uuid_df['time'].min()
    run_end_time = run_uuid_df['time'].max()
    total_run_time = (run_end_time - run_start_time).total_seconds()
    total_distance_1 = run_uuid_df['d1'].abs().sum() if 'd1' in run_uuid_df.columns else 0
    total_distance_2 = run_uuid_df['d2'].abs().sum() if 'd2' in run_uuid_df.columns else 0
    return {
        'run_uuid': run_uuid,
        'run_start_time': run_start_time.isoformat(),
        'run_end_time': run_end_time.isoformat(),
        'total_run_time': total_run_time,
        'total_distance_1': total_distance_1,
        'total_distance_2': total_distance_2,
    }


def main():
    """
    For the purposes of this example we will assume that the data is small enough to fit
    in memory on a single machine, so we will not use any distributed processing frameworks/techniques.
    """
    os.makedirs('output', exist_ok=True)
    df = pd.read_parquet(sample_data_url)
    df = preprocess_df(df)
    run_uuid_df_map = get_dfs_by_run_uuid(df)

    dbdf = db.from_sequence(list(run_uuid_df_map.items()))
    dbdf = dbdf.map(lambda run_uuid_df_tup: (run_uuid_df_tup[0], convert_to_features(run_uuid_df_tup[1])))
    dbdf = dbdf.map(lambda run_uuid_df_tup: (run_uuid_df_tup[0], match_timestamps_with_measurements(run_uuid_df_tup[1])))
    dbdf = dbdf.map(lambda run_uuid_df_tup: (run_uuid_df_tup[0], add_engineered_features(run_uuid_df_tup[1])))
    dbdf = dbdf.map(lambda run_uuid_df_tup: (run_uuid_df_tup[0], serialize_run_data_to_file(run_uuid_df_tup[0], run_uuid_df_tup[1])))
    ddf_run_stats = dbdf.map(lambda run_uuid_df_tup: calculate_run_stats(run_uuid_df_tup)).compute()

    pd.DataFrame.from_records(ddf_run_stats).to_csv('output/run_summary.csv', index=False)


if __name__ == '__main__':
    main()


