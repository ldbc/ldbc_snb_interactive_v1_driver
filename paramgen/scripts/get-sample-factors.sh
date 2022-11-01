#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

rm -f *.zip
rm -rf social-network-sf0.003*/
rm -rf scratch/factors
mkdir -p scratch/factors
wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/social-network-sf0.003-bi-factors.zip
unzip -q social-network-sf0.003-bi-factors.zip

rm -rf scratch/sample-data
mkdir -p scratch/sample-data
wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/social-network-sf0.003-bi-composite-merged-fk.zip
unzip -q social-network-sf0.003-bi-composite-merged-fk.zip

mv social-network-sf0.003-bi-factors/factors/parquet/raw/composite-merged-fk/* scratch/factors/
mv social-network-sf0.003-bi-composite-merged-fk scratch/sample-data/
