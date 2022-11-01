#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

rm -rf {deletes,inserts}
mkdir {deletes,inserts}

echo "##### Generate Update Streams #####"
./create_update_streams.py --raw_parquet_dir ${LDBC_SNB_DATA_ROOT_DIRECTORY} --output_dir ./ --batch_size_in_days 1
