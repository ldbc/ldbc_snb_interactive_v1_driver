from path_selection import PathCuration
import duckdb
from datetime import timedelta, datetime, timezone
import glob
import os
from pathlib import Path
import argparse
import logging

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
    "Q_14a" : ["person1Id", "person2Id"]
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
        logging_level:str='INFO'
    ):
        self.factor_tables_dir = factor_tables_dir
        self.raw_parquet_dir = raw_parquet_dir
        self.start_date = start_date
        self.end_date = end_date
        self.time_bucket_size_in_days = time_bucket_size_in_days
        self.generate_short_query_parameters = generate_short_query_parameters

        Path('scratch/paramgen.duckdb').unlink(missing_ok=True)
        self.cursor = duckdb.connect(database="scratch/paramgen.duckdb")

        # Set logger for class
        self.logger = logging.Logger(
            name="Paramgen",
            level=logging_level
        )

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

        self.logger.debug(f"Loading factor tables from path {factor_tables_path}")

        with open("schema.sql") as f:
            schema_def = f.read()
            self.cursor.execute(schema_def)

        self.logger.debug("============ Loading the factor tables ============")
        directories = glob.glob(f'{factor_tables_path}')
        if (len(directories) == 0):
            self.logger.error(f"{factor_tables_path} is empty")
            raise ValueError(f"{factor_tables_path} is empty")
        # Create views of raw parquet files
        for directory in directories:
            path_dir = Path(directory)
            if path_dir.is_dir():
                self.logger.debug(f"Loading {path_dir.name}")
                self.cursor.execute(f"DROP VIEW IF EXISTS {path_dir.name}")
                self.cursor.execute(
                    f"""
                    CREATE VIEW {path_dir.name} AS
                    SELECT * FROM read_parquet('{str(Path(directory).absolute()) + "/*.parquet"}');
                    """
                )
        self.logger.debug("============ Factor Tables loaded ============")

    def run(self):
        """
        Entry point of the parameter generation. Generates parameters
        for LDBC SNB Interactive queries.
        """
        # Create folder in case it does not exist.
        Path(f"{self.factor_tables_dir}/people4Hops").mkdir(parents=True, exist_ok=True)
        parquet_output_dir = f"{self.factor_tables_dir}/people4Hops/curated_paths.parquet"
        # Remove existing altered factor table
        Path(parquet_output_dir).unlink(missing_ok=True)

        self.logger.debug("============ Generate People 4 Hops ============")
        # The path curation is ran first and replaces the people4hops parquet file (old one is removed)
        # This to ensure 13b and 14b uses existing paths
        path_curation = PathCuration(self.raw_parquet_dir, self.factor_tables_dir)
        path_curation.get_people_4_hops_paths(self.start_date, self.end_date, 1, parquet_output_dir)
        self.logger.debug("============ Done ============")
        files = glob.glob('scratch/factors/people4Hops/*')
        for f in files:
            print(f)
            if f != 'scratch/factors/people4Hops/curated_paths.parquet':
                os.remove(f)
        self.create_views_of_factor_tables(self.factor_tables_dir)

        # The path queries are generated separately since path curation already contains
        # useFrom and useUntil columns for each parameter pair.
        self.logger.debug("============ Generate 13b and 14b parameters ============")
        self.generate_parameter_for_query_type(self.start_date, self.start_date, "13b")
        self.generate_parameter_for_query_type(self.start_date, self.start_date, "14b")
        self.logger.debug("============ Done ============")

        # Generate the other parameters
        self.logger.debug("============ Generate parameters Q1 - Q12 ============")
        self.generate_parameters(
            self.start_date,
            self.start_date,
            self.end_date,
            timedelta(days=self.time_bucket_size_in_days)
        )
        self.logger.debug("============ Done ============")
        self.logger.debug("============ Export parameters to parquet files ============")
        self.export_parameters()
        self.logger.debug("============ Done ============")

        self.logger.debug("============ Generate short read debug parameters ============")
        if (self.generate_short_query_parameters):
            self.generate_short_parameters()
        self.logger.debug("============ Done ============")

        # Remove temporary database
        Path('paramgen.snb.db').unlink(missing_ok=True)


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
        self.logger.info("Start time of initial_snapshot: " + str(date_limit))
        self.logger.info("End time of initial_snapshot: "   + str(end_date))
        self.logger.info("Time bucket size: "               + str(window_time))

        while (date_limit < end_date):
            self.logger.info("============ Generating parameters ============")
            for query_variant in ["1", "2", "3a", "3b", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13a", "14a"]:
                self.logger.info(f"- Q{query_variant}, date {date_limit.strftime('%Y-%m-%d')}")
                self.generate_parameter_for_query_type(date_limit, date_start, query_variant)
            date_limit = date_limit + window_time


    def generate_short_parameters(self):
        """
        Generates personIds and messageIds for manual testing of short queries
        Args:
            - cursor      (DuckDBPyConnection): cursor to the DuckDB instance
            - date_start  (datetime): The first day of the inserts. This is used for parameters that do not contain creation and deletion dates
        """
        self.logger.info("============ Generate Short Query Parameters ============")
        for query_variant in ["personId", "messageId"]:
            self.generate_parameter_for_query_type(self.start_date, self.start_date, query_variant)
            self.logger.info(f"- Q{query_variant} TO ../parameters/interactive-{query_variant}.parquet")
            self.cursor.execute(f"COPY 'Q_{query_variant}' TO '../parameters/interactive-{query_variant}.parquet' WITH (FORMAT PARQUET);")
        self.logger.info("============ Short Query Parameters exported ============")

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
        self.logger.info("============ Output parameters ============")
        for query_variant in ["1", "2", "3a", "3b", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13a", "14a"]:
            self.logger.info(f"- Q{query_variant} TO ../parameters/interactive-{query_variant}.parquet")
            column_ids = sort_columns_dict[f'Q_{query_variant}']
            parameter_df = self.cursor.execute(f"SELECT * FROM Q_{query_variant};").fetch_df()
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
            self.cursor.execute(f"CREATE TABLE Q_{query_variant}_filtered AS SELECT * FROM df_out")
            self.cursor.execute(f"COPY 'Q_{query_variant}_filtered' TO '../parameters/interactive-{query_variant}.parquet' WITH (FORMAT PARQUET);")

        for query_variant in ["13b", "14b"]:
            self.logger.info(f"- Q{query_variant} TO ../parameters/interactive-{query_variant}.parquet")
            self.cursor.execute(f"COPY 'Q_{query_variant}' TO '../parameters/interactive-{query_variant}.parquet' WITH (FORMAT PARQUET);")
        self.logger.info("============ Parameters exported ============")


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
        help="factor_tables_dir: directory containing the factor tables e.g. '/data/out-sf1'",
        type=str,
        default='factors/',
        required=False
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
        type=bool,
        default=False,
        required=False
    )
    args = parser.parse_args()

    start_date = datetime(year=2012, month=11, day=27, tzinfo=timezone.utc)
    end_date = datetime(year=2013, month=1, day=1, tzinfo=timezone.utc)

    PG = ParameterGeneration(args.factor_tables_dir, args.raw_parquet_dir, start_date, end_date, args.time_bucket_size_in_days, args.generate_short_query_parameters)
    PG.run()
