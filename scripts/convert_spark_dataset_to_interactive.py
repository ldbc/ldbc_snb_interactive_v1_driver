#!/usr/bin/env python3
"""
FILE: convert_spark_dataset_to_interactive.py
DESC: This file concatenates the insert and deletes streams from the Spark Datagen
      into one file per event. The new files are placed under a 
      newly created directory 'deletes' at the directory where the script
      is executed with the following files:
      - Comment.parquet
      - Forum.parquet
      - Forum_hasMember_Person.parquet
      - Person.parquet
      - Person_knows_Person.parquet
      - Person_likes_Comment.parquet
      - Person_likes_Post.parquet
      - Post.parquet
"""
import argparse
import glob
import os
import duckdb
import re
import time


class MergeBatchToSingleParquet:

    entities = [
        "Comment",                # INS7
        "Forum",                  # INS4
        "Forum_hasMember_Person", # INS5
        "Person",                 # INS1
        "Person_knows_Person",    # INS8
        "Person_likes_Comment",   # INS3
        "Person_likes_Post",      # INS2
        "Post"                    # INS6
    ]

    def __init__(self, input_dir, output_dir, input_type, data_format):
        """
        Args:
            - input_dir  (str): The root input dir (e.g. '/data/out-sf1')
            - output_dir (str): The output directory where the 'inserts' directory will be created
        """
        self.input_dir = input_dir
        self.database_name = 'snb.duckdb'
        self.output_dir = output_dir
        if (input_type == 'csv'):
            self.input_extension = '**/*.csv'
            self.input_path_folder = 'csv'
            self.input_format = "DELIMITER '|', HEADER, TIMESTAMPFORMAT '%Y-%m-%dT%H:%M:%S.%g+00:00'"
        elif (input_type == 'csv.gz'):
            self.input_extension = '**/*.csv.gz'
            self.input_path_folder = 'csv'
            self.input_format = "DELIMITER '|', HEADER, TIMESTAMPFORMAT '%Y-%m-%dT%H:%M:%S.%g+00:00'"
        elif (input_type == 'parquet'):
            self.input_extension = '**/*.snappy.parquet'
            self.input_path_folder = 'parquet'
            self.input_format = "FORMAT PARQUET"
        else:
            raise ValueError(f"Only 'csv', 'csv.gz' or 'parquet' input type are supported. Got '{input_type}'")

        self.data_format = data_format

        if (os.path.isfile(self.database_name)):
            os.remove(self.database_name)

        self.con = duckdb.connect(database=self.database_name)

    def convert_inserts(self):

        print(f"===== Inserts =====")

        with open("schema.sql") as f:
            schema_def = f.read()
            self.con.execute(schema_def)

        data_path = os.path.join(self.input_dir, f"graphs/{self.input_path_folder}/bi/{self.data_format}/inserts/dynamic")

        for entity in [
            "Comment",
            "Comment_hasTag_Tag",
            "Forum",
            "Forum_hasMember_Person",
            "Forum_hasTag_Tag",
            "Person",
            "Person_hasInterest_Tag",
            "Person_knows_Person",
            "Person_likes_Comment",
            "Person_likes_Post",
            "Person_studyAt_University",
            "Person_workAt_Company",
            "Post",
            "Post_hasTag_Tag"
        ]:
            print(f"-> {entity}")
            entity_dir = os.path.join(data_path, entity)
            print(entity_dir)

            if (not os.path.isdir(entity_dir)):
                raise ValueError(f"Directory {entity_dir} does not exist.")
            print(f'{entity_dir}/{self.input_extension}')
            for file_path in glob.glob(f'{entity_dir}/{self.input_extension}', recursive=True):
                print(file_path)
                if (not os.path.isfile(file_path)):
                    raise ValueError(f"File {file_path} does not exist.")
                self.con.execute(f"COPY {entity} FROM '{file_path}' ({self.input_format});")
        print("Loading finished.")

        self.run_script("convert_spark_inserts_to_interactive.sql")

        output_path = os.path.join(self.output_dir, "inserts")
        os.makedirs(output_path, exist_ok=True)

        for entity in self.entities:
            filename = os.path.join(output_path, entity + ".parquet")
            self.con.execute(f"COPY (SELECT date_part('epoch', creationDate)*1000+date_part('milliseconds', creationDate)%1000 AS creationDate, * EXCLUDE creationDate FROM {entity}_Insert_Converted) TO '{filename}' (FORMAT PARQUET)")
            print(f"Created {filename}")

    def convert_deletes(self):
        print(f"===== Deletes =====")

        with open("schema.sql") as f:
            schema_def = f.read()
            self.con.execute(schema_def)

        data_path = os.path.join(self.input_dir, f"graphs/{self.input_path_folder}/bi/{self.data_format}/deletes/dynamic")

        for entity in [
            "Comment",
            "Forum",
            "Forum_hasMember_Person",
            "Person",
            "Person_knows_Person",
            "Person_likes_Comment",
            "Person_likes_Post",
            "Post",
        ]:
            print(f"-> {entity}")
            entity_dir = os.path.join(data_path, entity)
            if (not os.path.isdir(entity_dir)):
                raise ValueError(f"Directory {entity_dir} does not exist.")

            for file_path in glob.glob(f'{entity_dir}/{self.input_extension}', recursive=True):
                print(file_path)
                if (not os.path.isfile(file_path)):
                    raise ValueError(f"File {file_path} does not exist.")
                self.con.execute(f"COPY {entity}_Delete FROM '{file_path}' ({self.input_format});")
        print("Loading finished.")

        output_path = os.path.join(self.output_dir, "deletes")
        os.makedirs(output_path, exist_ok=True)

        for entity in self.entities:
            filename = os.path.join(output_path, entity + ".parquet")
            self.con.execute(f"COPY (SELECT date_part('epoch', deletionDate)*1000+date_part('milliseconds', deletionDate)%1000 AS deletionDate, * EXCLUDE deletionDate FROM {entity}_Delete) TO '{filename}' (FORMAT PARQUET)")
            print(f"Created {filename}")

    def run_script(self, filename):
        with open(filename, "r") as f:
            queries_file = f.read()
            # strip comments
            queries_file = re.sub(r"\n--.*", "", queries_file)
            queries = queries_file.split(';\n') # split on semicolon-newline sequences
            for query in queries:
                if not query or query.isspace():
                    continue

                sql_statement = re.findall(r"^((CREATE|INSERT|DROP|DELETE|SELECT|COPY|UPDATE|ALTER) [A-Za-z0-9_ ]*)", query, re.MULTILINE)
                print(f"{sql_statement[0][0].strip()} ...")
                start = time.time()
                self.con.execute(query)
                self.con.commit()
                end = time.time()
                duration = end - start
                print(f"-> {duration:.4f} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_dir',
        help="input_dir: directory containing the data e.g. '/data/out-sf1'",
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
        '--input_type',
        help="input_type: input file type for update streams (csv.gz|csv|parquet)",
        type=str,
        required=True
    )

    args = parser.parse_args()

    Merger = MergeBatchToSingleParquet(args.input_dir, args.output_dir, args.input_type)

    Merger.convert_inserts()
    Merger.convert_deletes()
    print("Files combined & processed.")
