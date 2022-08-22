#!/usr/bin/env python3
"""
Script to convert the update stream csvs to parquet and add the dependency time
"""

from add_dependent_time_interactive import DependentTimeAppender
from convert_spark_dataset_to_interactive import MergeBatchToSingleParquet
import argparse
import os

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
        help="output_dir: directory containing the data e.g. '/data/out-sf1'",
        type=str,
        required=True
    )
    parser.add_argument(
        '--input_type',
        help="input_type: input file type for update streams (csv|parquet)",
        type=str,
        required=True
    )
    parser.add_argument(
        '--data_format',
        help="data_format: data format to convert (composite-merged-fk|composite-projected-fk)",
        type=str,
        required=True
    )
    args = parser.parse_args()

    Merger = MergeBatchToSingleParquet(args.input_dir, args.output_dir, args.input_type, args.data_format)
    Merger.convert_inserts()
    Merger.convert_deletes()

    dta_data_path = os.path.join(args.input_dir, f'graphs/parquet/raw/composite-merged-fk')
    DTA = DependentTimeAppender(
        input_file_path=args.output_dir,
        raw_data_path=dta_data_path
    )
    DTA.create_views()
    DTA.create_and_load_temp_tables()
    print("Data converted and dependent time added.")
