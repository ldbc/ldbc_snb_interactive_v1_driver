#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

rm -rf {deletes,inserts}
mkdir {deletes,inserts}

echo "Convert Datagen data sets in ${LDBC_SNB_DATA_ROOT_DIRECTORY}"
./convert_spark_dataset_to_interactive.py --input_dir ${LDBC_SNB_DATA_ROOT_DIRECTORY} --output_dir .
