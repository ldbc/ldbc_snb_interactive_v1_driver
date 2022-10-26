"""
FILE: update_interleave_calculation.py
This file contains functions to determine the event statistics of update streams.
Output:
    MaxDate: (long)
    MinDate: (long)
    Interleave: (float)
    Total Events: (int)
"""

import duckdb
import pandas as pd
import glob
import os
import argparse

def get_statistics_of_operation_stream(operations_df, date_column, operation_type, update_type):
    operations_min_date = operations_df[date_column].min()
    operations_max_date = operations_df[date_column].max()
    operations_df.rename(columns={date_column: "scheduledTime"}, inplace=True)
    operations_df['scheduledTime'] = pd.to_datetime(operations_df['scheduledTime'], unit='ms')
    operations_per_hour = operations_df.groupby([pd.Grouper(key='scheduledTime', freq='H')]).size().reset_index(name='count')
    return {
        'scheduledHourlyTime':operations_per_hour['scheduledTime'],
        'scheduledTime':operations_df['scheduledTime'],
        'minDate':operations_min_date,
        'maxDate':operations_max_date,
        'count': operations_per_hour['count'],
        'query': operation_type,
        'type' : update_type
    }

def get_timedeltas_between_operations(operations):
    timedeltas_operations = operations['scheduledTime'].diff().describe()
    timedeltas_operations['type'] = operations['type']
    timedeltas_operations['query'] = operations['query']
    return timedeltas_operations

def run(data_path, scale_factor):
    cursor = duckdb.connect()
    list_of_operations = []
    timedeltas_stats = []
    for update_type in ['deletes', 'inserts']:
        paths = glob.glob(f'{data_path}/update-streams-{scale_factor}/{update_type}/*.parquet')
        if update_type == 'deletes':
            date_column = 'deletionDate'
        else:
            date_column = 'creationDate'
        for path in paths:
            # Load the data using DuckDB
            operation_type = os.path.basename(path.removesuffix('.parquet'))
            cursor.execute("CREATE TABLE " + update_type + "_" + operation_type + " AS SELECT * FROM read_parquet('" + path + "');")
            operations_df = cursor.execute("SELECT * FROM read_parquet('" + path + "');").fetch_df()

            statistics = get_statistics_of_operation_stream(operations_df, date_column, operation_type, update_type)
            list_of_operations.append(statistics)
            timedeltas_stats.append(get_timedeltas_between_operations(statistics))

    max_dates = []
    min_dates = []
    counts = []
    for operation in list_of_operations:
        min_dates.append(operation['minDate'])
        counts.append(operation['count'].sum())
        print(operation['count'].sum())
    for operation in list_of_operations:
        max_dates.append(operation['maxDate'])
    print("MaxDate: " + str(max(max_dates)))
    print("MinDate: " + str(min(min_dates)))
    print("Interleave: " + str((max(max_dates) - min(min_dates)) / sum(counts)))
    print("Total Events: " + str(sum(counts)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_dir',
        help="data_dir: directory containing the update-stream-folder e.g. '/mnt/ldbc_snb_interactive_impls/'",
        type=str,
        required=True
    )
    parser.add_argument(
        '--scale_factor',
        help="scale_factor: scale factor e.g. 'SF1'",
        type=str,
        required=True
    )
    args = parser.parse_args()

    run(args.data_path, args.scale_factor)