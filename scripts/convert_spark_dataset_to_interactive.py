#!/usr/bin/env python3
"""
FILE: convert_spark_dataset_to_interactive.py
DESC: This file concatenates the insert and deletes streams from the Spark Datagen
      into one file per event. The new files are placed under a 
      newly created directory 'deletes' at the directory where the script
      is executed with the following files:
      - Comment.csv
      - Forum.csv
      - Forum_hasMember_Person.csv
      - Person.csv
      - Person_knows_Person.csv
      - Person_likes_Comment.csv
      - Person_likes_Post.csv
      - Post.csv
"""
import argparse
import glob
import os
import duckdb
import re
import time

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

def run_script(con, filename):
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
            con.execute(query)
            con.commit()
            end = time.time()
            duration = end - start
            print(f"-> {duration:.4f} seconds")

def convert_inserts(input_dir, output_dir):
    """
    Args:
        - input_dir  (str): The root input dir (e.g. '/data/out-sf1')
        - output_dir (str): The output directory where the 'inserts' directory will be created
    """
    print(f"===== Inserts =====")

    con = duckdb.connect(database='snb.duckdb')

    with open("schema.sql") as f:
        schema_def = f.read()
        con.execute(schema_def)

    data_path = os.path.join(input_dir, "graphs/csv/bi/composite-merged-fk/inserts/dynamic")

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
        for csv_path in glob.glob(f'{entity_dir}/**/*.csv', recursive=True):
            con.execute(f"COPY {entity} FROM '{csv_path}' (DELIMITER '|', HEADER, TIMESTAMPFORMAT '%Y-%m-%dT%H:%M:%S.%g+00:00');")
    print("Loading finished.")

    run_script(con, "convert_spark_inserts_to_interactive.sql")

    output_path = os.path.join(output_dir, "inserts")
    os.makedirs(output_path, exist_ok=True)

    for entity in entities:
        filename = os.path.join(output_path, entity + ".parquet")
        con.execute(f"COPY (SELECT date_part('epoch', creationDate)*1000+date_part('milliseconds', creationDate)%1000, * EXCLUDE creationDate FROM {entity}_Insert_Converted) TO '{filename}' (FORMAT PARQUET)")

def convert_deletes(input_dir, output_dir):
    """
    Args:
        - input_dir  (str): The root input dir (e.g. '/data/out-sf1')
        - output_dir (str): The output directory where the 'deletes' directory will be created
    """
    print(f"===== Deletes =====")

    con = duckdb.connect(database='snb.duckdb')

    with open("schema.sql") as f:
        schema_def = f.read()
        con.execute(schema_def)

    data_path = os.path.join(input_dir, "graphs/csv/bi/composite-merged-fk/deletes/dynamic")

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
        for csv_path in glob.glob(f'{entity_dir}/**/*.csv', recursive=True):
            con.execute(f"COPY {entity}_Delete FROM '{csv_path}' (DELIMITER '|', HEADER, TIMESTAMPFORMAT '%Y-%m-%dT%H:%M:%S.%g+00:00');")
    print("Loading finished.")

    output_path = os.path.join(output_dir, "deletes")
    os.makedirs(output_path, exist_ok=True)

    for entity in entities:
        filename = os.path.join(output_path, entity + ".parquet")
        con.execute(f"COPY (SELECT date_part('epoch', deletionDate)*1000+date_part('milliseconds', deletionDate)%1000 AS deletionDate, * EXCLUDE deletionDate FROM {entity}_Delete) TO '{filename}' (FORMAT PARQUET)")

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
    args = parser.parse_args()

    convert_inserts(args.input_dir, args.output_dir)
    convert_deletes(args.input_dir, args.output_dir)
    print("Files combined & processed.")
