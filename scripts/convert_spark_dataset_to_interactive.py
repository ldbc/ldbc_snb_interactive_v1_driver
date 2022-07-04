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
import pandas as pd
import glob
import os
import duckdb

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

def merge_csvs_of_event(csv_path) -> pd.DataFrame:
    """
    Args:
        - csv_path (str): Path to the root folder with csv.gz files to combine
                          Point this to e.g. Person_likes_Post.
    Returns:
        pd.DataFrame containing concatenated CSV-data.
    """
    list_of_dfs = []
    for csv_file in glob.glob(f'{csv_path}/**/*.csv', recursive=True):
        print(csv_file)
        df = pd.read_csv(csv_file, delimiter='|')
        list_of_dfs.append(df)

    df = pd.concat(list_of_dfs)
    df = df.sort_values('deletionDate')
    df = df.reset_index(drop=True)
    return df

def convert_inserts(input_dir, output_dir):
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
        print(f"===== {entity} =====")
        entity_dir = os.path.join(data_path, entity)
        print(f"--> {entity_dir}")
        for csv_path in glob.glob(f'{entity_dir}/**/*.csv', recursive=True):
            print(csv_path)
            con.execute(f"COPY {entity} FROM '{csv_path}' (DELIMITER '|', HEADER, TIMESTAMPFORMAT '%Y-%m-%dT%H:%M:%S.%g+00:00');")

    with open("convert_spark_inserts_to_interactive.sql") as f:
        convert_script = f.read()
        con.execute(convert_script)

    output_path = os.path.join(output_dir, "inserts")

    os.makedirs(output_path, exist_ok=True)

    for entity in entities:
        filename = os.path.join(output_path, entity + ".csv")
        con.execute(f"""
            COPY
                (SELECT strftime(creationDate::timestamp, '%Y-%m-%dT%H:%M:%S.%g+00:00') AS creationDate, * EXCLUDE creationDate FROM {entity}_Update)
                TO '{filename}'
                (DELIMITER '|', HEADER)
            """)

def convert_deletes(input_dir, output_dir):
    """
    Args:
        - input_dir  (str): The root input dir (e.g. '/data/out-sf1') 
        - output_dir (str): 
    """
    update_path = os.path.join(input_dir, "graphs/csv/bi/composite-merged-fk/deletes/dynamic")
    output_path = os.path.join(output_dir, "deletes")

    os.makedirs(output_path, exist_ok=True)
    # The entity name is the folder name, also used for the final csv filename
    for folder_name in entities:
        print(f"Process folder: {folder_name}")
        df = merge_csvs_of_event(os.path.join(update_path, folder_name))
        output_file = os.path.join(output_dir, "deletes", folder_name +".csv")
        df.to_csv(f"{output_file}", sep='|', index=False);


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
    print("Files combined")
