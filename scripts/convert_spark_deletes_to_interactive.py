#!/usr/bin/env python3
"""
FILE: convert_spark_deletes_to_interactive.py
DESC: This file concatenates the deletes streams from the Spark Datagen
      into one file per delete event. The new files are placed under a 
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

import pandas as pd
import glob
import os
import argparse

def main(root_folder):
    """
    Args:
        - root_folder (str): unpacked directory containing the data
    """
    update_type = 'deletes'
    update_path = f"{root_folder}/graphs/csv/bi/composite-merged-fk/{update_type}/dynamic/"

    os.makedirs('deletes', exist_ok=True)

    list_of_folders = [
        "Comment",
        "Forum",
        "Forum_hasMember_Person",
        "Person",
        "Person_knows_Person",
        "Person_likes_Comment",
        "Person_likes_Post",
        "Post"
    ]

    for folder in list_of_folders:
        print(f"Process folder: {folder}")
        df = merge_csvs_of_event(update_path + f"{folder}")
        df.to_csv(f"{update_type}/{folder}.csv", sep='|', index=False)

    print("Files combined")


def merge_csvs_of_event(csv_path) -> pd.DataFrame:
    """
    Args:
        - csv_path (str): Path to the root folder with csv.gz files to combine
                          Point this to e.g. Person_likes_Post.
    Returns:
        pd.DataFrame containing concatenated CSV-data.
    """
    list_of_dfs = []
    print(csv_path)
    for csv_file in glob.glob(f'{csv_path}/**/*.csv', recursive=True):
        print(csv_file)
        df = pd.read_csv(csv_file, delimiter='|')
        list_of_dfs.append(df)
    
    df = pd.concat(list_of_dfs)
    df = df.sort_values('deletionDate')
    df = df.reset_index(drop=True)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "root_folder",
        help="root_folder of the directory containing the data e.g. '/data/out-sf1'",
        type=str)
    args = parser.parse_args()
    main(args.root_folder)
