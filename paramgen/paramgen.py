#!/usr/bin/env python3
"""
FILE: paramgen.py
DESC: This file contains the class, ParameterGeneration, containing functions
      to generate parameters for the LDBC SNB Interactive v2.0 workload. This
      script requires Python 3.10 or higher.

      The parameter generation goes through the following steps:
      1. Regenerate people4Hops table, containing 4-hop paths between two
         persons with a useFrom and useUntil date.
      2. Preselect windows for selected factor tables
      3. Generate parameters
      4. Generate parameters for short queries (used for debugging)

      Output are parquet files containing the parameters per query, with a 
      start and end date.
"""
from path_selection import PathCuration
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import numpy as np
import os
import duckdb
import glob
import argparse
import logging
import time
import json

sort_columns_dict = {
    "Q_1"   : ["personId", "firstName"],
    "Q_2"   : ["personId", "maxDate"],
    "Q_3a"  : ["personId", "countryXName", "countryYName", "startDate", "durationDays"],
    "Q_3b"  : ["personId", "countryXName", "countryYName", "startDate", "durationDays"],
    "Q_4"   : ["personId", "startDate", "durationDays"],
    "Q_5"   : ["personId", "minDate"],
    "Q_6"   : ["personId", "tagName"],
    "Q_7"   : ["personId"],
    "Q_8"   : ["personId"],
    "Q_9"   : ["personId", "maxDate"],
    "Q_10"  : ["personId", "month"],
    "Q_11"  : ["personId", "countryName", "workFromYear"],
    "Q_12"  : ["personId", "tagClassName"],
    "Q_13a" : ["person1Id", "person2Id"],
    "Q_13b" : ["person1Id", "person2Id"],
    "Q_14a" : ["person1Id", "person2Id"],
    "Q_14b" : ["person1Id", "person2Id"]
}

class ParameterGeneration():

    def __init__(
        self,
        factor_tables_dir:str,
        raw_parquet_dir:str,
        start_date:datetime,
        end_date:datetime,
        time_bucket_size_in_days:int,
        generate_short_query_parameters:bool,
        threshold_values_path:str,
        logging_level:str='INFO'
    ):
        self.factor_tables_dir = factor_tables_dir
        self.raw_parquet_dir = raw_parquet_dir
        self.start_date = start_date
        self.end_date = end_date
        self.time_bucket_size_in_days = time_bucket_size_in_days
        self.generate_short_query_parameters = generate_short_query_parameters
        self.threshold_values_path = threshold_values_path
        Path('scratch/paramgen.duckdb').unlink(missing_ok=True)
        self.cursor = duckdb.connect(database="scratch/paramgen.duckdb")

        # Set logger for class
        self.logger = logging.Logger(
            name="Paramgen",
            level=logging_level
        )


    def define_threshold_per_table(self, param_list):

        for params in param_list:
            threshold_default = int(params['threshold'])
            thresholds = np.arange(threshold_default, threshold_default * 100, threshold_default)
            for threshold in thresholds:
                threshold_valid = False
                # First, try the default parameters.
                # If nothing is returned, increase threshold until group with 
                # minimum size is returned
                with open(f"paramgen-prepare/{params['template_file']}", "r") as parameter_query_file:
                    query_template = parameter_query_file.read()
                    query_template = query_template.replace(':threshold',       str(threshold))
                    query_template = query_template.replace(':min_occurence',   str(int(params['min_occurence'])))
                    query_template = query_template.replace(':min_param_value', str(int(params['min_param_value'])))
                    query_template = query_template.replace(':param_column',    str(params['param_column']))
                    query_template = query_template.replace(':source_table',    str(params['source_table']))
                    query_template = query_template.replace(':window_column',   str(params['window_column']))
                    results = self.cursor.execute(f"SELECT * FROM ({query_template});").fetchall()
                    if (len(results) > 0):
                        print(f"Threshold updated from {threshold_default} to {threshold} for table {params['source_table']}")
                        params['threshold'] = threshold
                        threshold_valid = True
                        break
            if threshold_valid == False:
                raise ValueError(
                    f"""
                    Error when determining threshold: no parameters are returned for table {params['source_table']}
                    on column {params['param_column']}. Start threshold: {threshold_default} Max threshold: {threshold}
                    """
                )
        return param_list

    def prepare_factor_tables(self, param_list):
        """
        Prepare the num friends factor tables by preselecting the windows.
        Args:
            - param_list(list(dict)): A list of dictionaries with the following information:
                                      {
                                        'source_table'    : 'personNumFriendsOfFriendsOfFriends',
                                        'table_name'      : 'personNumFriendsSelected',
                                        'threshold'       : 5,
                                        'min_occurence'   : 100,
                                        'min_param_value' : 3000,
                                        'window_column'    : 'numFriends',
                                        'param_column'    : 'Person1Id'
                                      }
                                      The threshold is the maximum difference between neighbouring persons
                                      when the column, param_column. The minimum occurence is the minimum
                                      amount of parameters. Lastly, the min_param_value of the column to 
                                      take into account. This is to filter out trivial parameters.
        """
        param_list = self.define_threshold_per_table(param_list)

        for params in param_list:
            with open(f"paramgen-prepare/{params['template_file']}", "r") as parameter_query_file:
                query_template = parameter_query_file.read()
                query_template = query_template.replace(':threshold',       str(int(params['threshold'])))
                query_template = query_template.replace(':min_occurence',   str(int(params['min_occurence'])))
                query_template = query_template.replace(':min_param_value', str(int(params['min_param_value'])))
                query_template = query_template.replace(':param_column',    str(params['param_column']))
                query_template = query_template.replace(':source_table',    str(params['source_table']))
                query_template = query_template.replace(':window_column',   str(params['window_column']))
                self.cursor.execute(f"DROP TABLE IF EXISTS {str(params['table_name'])}")
                self.cursor.execute(f"CREATE TABLE {str(params['table_name'])} AS SELECT * FROM ({query_template});")


    def create_views_of_factor_tables(self, factor_tables_path):
        """
        Args:
            - cursor (DuckDBPyConnection): cursor to the DuckDB instance
            - factor_tables_path    (str): path to the factor tables. Only Unix paths are supported
            - preview_tables    (boolean): Whether the first five rows of the factor table should be shown.
        """

        if factor_tables_path[-1] != '*':
            if factor_tables_path[-1] != '/':
                factor_tables_path = factor_tables_path + '/*'
            else:
                factor_tables_path = factor_tables_path + '*'

        print(f"Loading factor tables from path {factor_tables_path}")

        with open("schema.sql") as f:
            schema_def = f.read()
            self.cursor.execute(schema_def)

        print("============ Loading the factor tables ============")
        directories = glob.glob(f'{factor_tables_path}')
        if (len(directories) == 0):
            self.logger.error(f"{factor_tables_path} is empty")
            raise ValueError(f"{factor_tables_path} is empty")
        # Create views of raw parquet files
        for directory in directories:
            path_dir = Path(directory)
            if path_dir.is_dir():
                print(f"Loading {path_dir.name}")
                self.cursor.execute(f"DROP VIEW IF EXISTS {path_dir.name}")
                self.cursor.execute(
                    f"""
                    CREATE VIEW {path_dir.name} AS
                    SELECT * FROM read_parquet('{str(Path(directory).absolute()) + "/*.parquet"}');
                    """
                )
        print("============ Factor Tables loaded ============")

    def run(self, generate_paths):
        """
        Entry point of the parameter generation. Generates parameters
        for LDBC SNB Interactive queries.
        """
        # Create folder in case it does not exist.
        print(f'Loading factor tables from {self.factor_tables_dir}')
        if generate_paths:
            Path(f"{self.factor_tables_dir}/people4Hops").mkdir(parents=True, exist_ok=True)
            parquet_output_dir = f"{self.factor_tables_dir}/people4Hops/curated_paths.parquet"
            # Remove existing altered factor table
            Path(parquet_output_dir).unlink(missing_ok=True)
            print("============ Generate People 4 Hops ============")

            # The path curation is ran first and replaces the people4hops parquet file (old one is removed)
            # This to ensure 13b and 14b uses existing paths
            path_curation = PathCuration(self.raw_parquet_dir, self.factor_tables_dir)
            path_curation.get_people_4_hops_paths(self.start_date, self.end_date, self.time_bucket_size_in_days, parquet_output_dir)
            print("============ Done ============")
            files = glob.glob(f'{self.factor_tables_dir}/people4Hops/*')
            for f in files:
                print(f)
                if f != f'{self.factor_tables_dir}/people4Hops/curated_paths.parquet':
                    os.remove(f)
        self.create_views_of_factor_tables(self.factor_tables_dir)

        with open(self.threshold_values_path) as json_file:
            prepare_tables_params = json.load(json_file)

        paramgen_start_time = time.time()

        self.prepare_factor_tables(prepare_tables_params)

        # The path queries are generated separately since path curation already contains
        # useFrom and useUntil columns for each parameter pair.
        print("============ Generate 13b and 14b parameters ============")
        # self.generate_parameter_for_query_type(self.start_date, self.start_date, "13b")
        self.generate_parameter_for_query_type(self.start_date, self.start_date, "14b")
        print("============ Done ============")

        # Generate the other parameters
        print("============ Generate parameters Q1 - Q13 ============")
        self.generate_parameters(
            self.start_date,
            self.start_date,
            self.end_date,
            timedelta(days=self.time_bucket_size_in_days)
        )
        print("============ Done ============")
        print("============ Export parameters to parquet files ============")
        self.export_parameters()
        print("============ Done ============")
        paramgen_end_time = time.time()
        print(f"Total Parameter Generation Duration: {paramgen_end_time - paramgen_start_time:.4f} seconds")
        print("============ Generate short read debug parameters ============")
        if (self.generate_short_query_parameters):
            self.generate_short_parameters()
        print("============ Done ============")

        # Remove temporary database
        Path('scratch/paramgen.duckdb').unlink(missing_ok=True)


    def generate_parameters(self, date_limit, date_start, end_date, window_time):
        """
        Generates paramters for all query types until end_date is reached.
        Args:
            - cursor      (DuckDBPyConnection): cursor to the DuckDB instance
            - date_limit  (datetime): The day to filter on. This date will be used to compare creation and deletion dates
            - date_start  (datetime): The first day of the inserts. This is used for parameters that do not contain creation and deletion dates
            - end_date    (datetime): The last day of the inserts and when the loop stops.
            - window_time (timedelta): 
        """
        print("Start time of initial_snapshot: " + str(date_limit))
        print("End time of initial_snapshot: "   + str(end_date))
        print("Time bucket size: "               + str(window_time))

        while (date_limit < end_date):
            # print("============ Generating parameters ============")
            for query_variant in ["1", "2", "3a", "3b", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13a", "13b", "14a", "14b"]:
                # print(f"- Q{query_variant}, date {date_limit.strftime('%Y-%m-%d')}")
                self.generate_parameter_for_query_type(date_limit, date_start, query_variant)
            date_limit = date_limit + window_time

    def generate_short_parameters(self):
        """
        Generates personIds and messageIds for manual testing of short queries
        Args:
            - cursor      (DuckDBPyConnection): cursor to the DuckDB instance
            - date_start  (datetime): The first day of the inserts. This is used for parameters that do not contain creation and deletion dates
        """
        print("============ Generate Short Query Parameters ============")
        for query_variant in ["personId", "messageId"]:
            self.generate_parameter_for_query_type(self.start_date, self.start_date, query_variant)
            print(f"- Q{query_variant} TO ../parameters/interactive-{query_variant}.parquet")
            self.cursor.execute(f"COPY 'Q_{query_variant}' TO '../parameters/interactive-{query_variant}.parquet' WITH (FORMAT PARQUET);")
        print("============ Short Query Parameters exported ============")

    def generate_parameter_for_query_type(self, date_limit, date_start, query_variant):
        """
        Creates parameter for given query variant.
        Args:
            - cursor (DuckDBPyConnection): cursor to the DuckDB instance
            - date_limit (datetime): The day to filter on. This date will be used to compare creation and deletion dates
            - date_start (datetime): The first day of the inserts. This is used for parameters that do not contain creation and deletion dates
            - query_variant (str): number of the query to generate the parameters
        """
        date_limit_string = date_limit.strftime('%Y-%m-%d')
        date_limit_long = date_limit.timestamp() * 1000
        date_start_long = date_start.timestamp() * 1000
        with open(f"paramgen-queries/pg-{query_variant}.sql", "r") as parameter_query_file:
            parameter_query = parameter_query_file.read().replace(':date_limit_filter', f'\'{date_limit_string}\'')
            parameter_query = parameter_query.replace(':date_limit_long', str(int(date_limit_long)))
            parameter_query = parameter_query.replace(':date_start_long', str(int(date_start_long)))
            self.cursor.execute(f"INSERT INTO 'Q_{query_variant}' SELECT * FROM ({parameter_query});")

    def export_parameters(self):
        """
        Export parameters to interactive-Q{query_variant}.parquet files
        Args:
            - cursor      (DuckDBPyConnection): cursor to the DuckDB instance
        """
        print("============ Output parameters ============")
        Path("../parameters/").mkdir(parents=True, exist_ok=True)
        for query_variant in ["1", "2", "3a", "3b", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13a", "13b", "14a", "14b"]:
            print(f"- Q{query_variant} TO ../parameters/interactive-{query_variant}.parquet")
            column_ids = sort_columns_dict[f'Q_{query_variant}']
            parameter_df = self.cursor.execute(f"SELECT * FROM Q_{query_variant};").fetch_df().drop_duplicates()
            parameter_df = parameter_df.sort_values(column_ids)
            groupby_series = []
            for column in column_ids:
                groupby_series.append(parameter_df[column])

            day_diff = (parameter_df['useFrom'] - parameter_df['useUntil'].groupby(groupby_series).shift()).dt.days
            group_no = (day_diff.isna() | day_diff.gt(1)).cumsum()
            aggregate_rules = {}
            for column in column_ids:
                aggregate_rules[column] = 'first'
            aggregate_rules['useFrom'] = 'first'
            aggregate_rules['useUntil'] = lambda x: x.iloc[-1]

            df_out = (parameter_df.groupby(column_ids + [group_no], dropna=False, as_index=False)
                .agg(aggregate_rules))
            self.cursor.execute(f"CREATE TABLE Q_{query_variant}_filtered AS SELECT * FROM df_out ORDER BY useFROM")
            self.cursor.execute(f"COPY 'Q_{query_variant}_filtered' TO '../parameters/interactive-{query_variant}.parquet' WITH (FORMAT PARQUET);")
        print("============ Parameters exported ============")

# https://stackoverflow.com/a/52403318/7014190
def str_to_bool(value):
    if value.lower() in {'false', 'f', '0', 'no', 'n'}:
        return False
    elif value.lower() in {'true', 't', '1', 'yes', 'y'}:
        return True
    raise ValueError(f'{value} is not a valid boolean value')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--raw_parquet_dir',
        help="raw_parquet_dir: directory containing the raw parquet file for Person and Person_knows_Person e.g. '/data/out-sf1/graphs/parquet/raw/'",
        type=str,
        required=True
    )
    parser.add_argument(
        '--factor_tables_dir',
        help="factor_tables_dir: directory containing the factor tables e.g. '/data/out-sf1/factors/parquet/raw/composite-merged-fk/'",
        type=str,
        required=True
    )
    parser.add_argument(
        '--time_bucket_size_in_days',
        help="time_bucket_size_in_days: How many days the parameters should include, e.g. 1",
        type=int,
        default=1,
        required=False
    )
    parser.add_argument(
        '--generate_short_query_parameters',
        help="generate_short_query_parameters: Generate parameters to use manually for the short queries (these are not loaded by the driver)",
        type=str_to_bool,
        default=False,
        nargs='?',
        required=False
    )
    parser.add_argument(
        '--generate_paths',
        help="generate_paths: Whether paths should be generated",
        type=str_to_bool,
        default=True,
        nargs='?',
        required=False
    )
    parser.add_argument(
        '--threshold_values_path',
        help="path to the threshold json file to use",
        type=str,
        required=True
    )

    args = parser.parse_args()

    start_date = datetime(year=2010, month=1, day=1, hour=0, minute=0, second=0, tzinfo=ZoneInfo('GMT')).timestamp()
    end_date = datetime(year=2013, month=1, day=1, hour=0, minute=0, second=0, tzinfo=ZoneInfo('GMT'))
    bulk_load_portion = 0.97
    threshold = datetime.fromtimestamp(end_date.timestamp() - ((end_date.timestamp() - start_date) * (1 - bulk_load_portion)), tz=ZoneInfo('GMT'))
    PG = ParameterGeneration(args.factor_tables_dir, args.raw_parquet_dir, threshold, end_date, args.time_bucket_size_in_days, args.generate_short_query_parameters, args.threshold_values_path)
    PG.run(args.generate_paths)
