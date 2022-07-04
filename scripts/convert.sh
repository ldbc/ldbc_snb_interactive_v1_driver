#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

rm -rf {deletes,inserts}
mkdir {deletes,inserts}

./convert_spark_dataset_to_interactive.py --input_dir ${DATA_ROOT_DIRECTORY} --output_dir .
