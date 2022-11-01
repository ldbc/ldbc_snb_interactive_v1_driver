#!/usr/bin/env python3
"""
FILE: create_update_streams.py
DESC: Creates the update streams for LDBC SNB Interactive using the raw parquet files
"""
import argparse
import glob
from multiprocessing.sharedctypes import Value
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import duckdb
import re
import time
import copy

class UpdateStreamCreator:

    def __init__(self, raw_parquet_dir, output_dir, start_date, end_date, batch_size_in_days):
        """
        Args:
            - raw_parquet_dir  (str): The root input dir (e.g. '/data/out-sf1')
            - output_dir (str): The output directory for the inserts and
                                deletes folder and files
            - start_date (datetime): Start date of the update streams
            - end_date (datetime): End date of the update streams
        """
        self.raw_parquet_dir = raw_parquet_dir
        self.output_dir = output_dir
        self.start_date = start_date
        self.end_date = end_date
        self.batch_size_in_days = batch_size_in_days
        self.database_name = 'snb.stream.duckdb'
        self.input_extension = '**/*.snappy.parquet'
        self.input_path_folder = 'parquet'

        Path(self.database_name).unlink(missing_ok=True) # Remove original file
        self.cursor = duckdb.connect(database=self.database_name)

    def check_person_tag_table(self):
        """
        Checks whether Person_hasInterest_Tag uses TagId or interestId
        """
        df = self.cursor.execute("SELECT * FROM Person_hasInterest_Tag LIMIT 1").fetch_df()
        if "TagId" in df.columns:
            return "TagId"
        elif "interestId" in df.columns:
            return "interestId"
        else:
            raise ValueError(f"Person_hasInterest_Tag has unknown id column. {df.columns}")

    def execute(self):

        print(f"===== Create Update Streams =====")
        Path(f"{self.output_dir}/inserts").mkdir(parents=True, exist_ok=True)
        Path(f"{self.output_dir}/deletes").mkdir(parents=True, exist_ok=True)

        tag_id_column = "TagId"

        directory = Path(f"{self.raw_parquet_dir}/composite-merged-fk")
        if not directory.exists ():
            raise ValueError(f"Provided directory does not contain expected composite-merged-fk folder. Got: {self.raw_parquet_dir}")

        # Get folders
        for folder in glob.glob(f"{self.raw_parquet_dir}/composite-merged-fk/**/*"):
            if (os.path.isdir(folder)):
                entity = folder.split('/')[-1]
                self.cursor.execute(f"CREATE OR REPLACE VIEW {entity} AS SELECT * FROM read_parquet('{folder}/*.parquet');")
                print(f"VIEW FOR {entity} CREATED")
                if entity == "Person_hasInterest_Tag":
                    tag_id_column = self.check_person_tag_table()

        start_date_long = self.start_date.timestamp() * 1000

        with open("dependant_time_queries.sql", "r") as f:
            queries_file = f.read()
            queries_file = queries_file.replace(':tag_column_name', str(tag_id_column))
            queries_file = queries_file.replace(':start_date_long', str(int(start_date_long)))
            queries_file = queries_file.replace(':output_dir', self.output_dir)

            # strip comments
            queries_file = re.sub(r"\n--.*", "", queries_file)
            queries = queries_file.split(';\n') # split on semicolon-newline sequences
            for query in queries:
                if not query or query.isspace():
                    continue

                print(query)
                start = time.time()
                self.cursor.execute(query)
                end = time.time()
                duration = end - start
                print(f"-> {duration:.4f} seconds")


        self.execute_batched()


    def execute_batched(self):
        """
        Executes a query batched using timedeltas. Used for Comment and Post tables
        that require a large amount of memory.
        Args:
            - days (int, optional): the amount of days (size of batch)
        """
        window_time = self.batch_size_in_days * 24 * 3600
        start_date = self.start_date.timestamp()
        end_date = self.end_date.timestamp()
        index = 0
        print(start_date)
        with open("dependant_time_queries_large.sql", "r") as f:
            queries_file = f.read()
            queries_file = queries_file.replace(':output_dir', self.output_dir)
            queries = queries_file.split(';\n') # split on semicolon-newline sequences
            for query in queries:
                if not query or query.isspace():
                    continue

                date_limit = copy.deepcopy(start_date)
                end_date_limit = end_date + window_time
                while (date_limit < end_date_limit):
                    date_start_long = date_limit * 1000
                    date_limit = date_limit + window_time
                    date_limit_long = date_limit * 1000
                    query_batched = query.replace(':start_date_long',       str(int(date_start_long))) 
                    query_batched = query_batched.replace(':end_date_long', str(int(date_limit_long)))
                    query_batched = query_batched.replace(':output_dir', self.output_dir)
                    query_batched = query_batched.replace(':index', str(int(index)))

                    start = time.time()
                    print(query_batched)
                    self.cursor.execute(query_batched)
                    end = time.time()
                    duration = end - start
                    print(f"-> {duration:.4f} seconds")

                    index = index + 1

            for entity in ["Post", "Comment", "Forum_hasMember_Person"]:
                self.cursor.execute(f"""
                    COPY(
                        SELECT * FROM read_parquet('{self.output_dir}/inserts/{entity}-*.parquet') ORDER BY creationDate
                    ) TO '{self.output_dir}/inserts/{entity}.parquet' (FORMAT 'parquet');
                    """
                )
                for temp_file in glob.glob(f"{self.output_dir}/inserts/{entity}-*.parquet"):
                    Path(temp_file).unlink()

            entity = "Forum_hasMember_Person"
            # Case for deletes:
            self.cursor.execute(f"""
                COPY(
                    SELECT * FROM read_parquet('{self.output_dir}/deletes/{entity}-*.parquet') ORDER BY deletionDate
                ) TO '{self.output_dir}/deletes/{entity}.parquet' (FORMAT 'parquet');
                """
            )
            for temp_file in glob.glob(f"{self.output_dir}/deletes/{entity}-*.parquet"):
                Path(temp_file).unlink()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--raw_parquet_dir',
        help="raw_parquet_dir: directory containing the data e.g. 'graphs/parquet/raw/'",
        type=str,
        required=True
    )
    parser.add_argument(
        '--output_dir',
        help="output_dir: folder to output the data",
        type=str,
        required=True
    )
    parser.add_argument(
        '--batch_size_in_days',
        help="batch_size_in_days: The amount of days in a batch",
        type=int,
        default=1,
        required=False
    )
    args = parser.parse_args()

    # Determine date boundaries
    start_date = datetime(year=2010, month=1, day=1, hour=0, minute=0, second=0, tzinfo=ZoneInfo('GMT')).timestamp()
    end_date = datetime(year=2013, month=1, day=1, hour=0, minute=0, second=0, tzinfo=ZoneInfo('GMT'))
    bulk_load_portion = 0.97

    threshold = datetime.fromtimestamp(end_date.timestamp() - ((end_date.timestamp() - start_date) * (1 - bulk_load_portion)), tz=ZoneInfo('GMT'))

    directory = Path(args.raw_parquet_dir)
    if not directory.exists ():
        raise ValueError(f"raw_parquet_dir does not exist. Got: {args.raw_parquet_dir}")

    start = time.time()
    USC = UpdateStreamCreator(args.raw_parquet_dir, args.output_dir, threshold, end_date, args.batch_size_in_days)
    USC.execute()
    end = time.time()
    duration = end - start
    print(f"-> {duration:.4f} seconds")
    print("Update Streams Created")
