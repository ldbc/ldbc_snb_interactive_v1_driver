#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

rm -rf {deletes,inserts}
mkdir {deletes,inserts}

echo "##### Generate insert and delete streams, add dependencyTime column #####"
./convert_and_append.py --input_dir ${LDBC_SNB_DATA_ROOT_DIRECTORY} --output_dir ./ --input_type ${DATA_INPUT_TYPE} --data_format composite-merged-fk
