"""
Script to convert the update stream csvs to parquet and add the dependency time
"""

from add_dependent_time_interactive import DependentTimeAppender
from convert_spark_dataset_to_interactive import convert_deletes, convert_inserts
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
    args = parser.parse_args()

    root_data_path = args.input_dir
    dta_data_path = os.path.join(root_data_path, 'graphs/parquet/raw/composite-merged-fk')

    convert_inserts(root_data_path, dta_data_path)
    convert_deletes(root_data_path, dta_data_path)

    DTA = DependentTimeAppender(dta_data_path)
    DTA.create_views()
    DTA.create_and_load_temp_tables()

