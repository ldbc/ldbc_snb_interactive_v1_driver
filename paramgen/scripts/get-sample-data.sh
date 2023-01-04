#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ../scratch

rm -f *.zip
rm -rf social-network-sf0.003*/
rm -rf sample-data
mkdir -p sample-data

wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/social-network-sf0.003-bi-factors.zip
unzip -q social-network-sf0.003-bi-factors.zip

wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/social-network-sf0.003-bi-composite-merged-fk.zip
unzip -q social-network-sf0.003-bi-composite-merged-fk.zip

mv social-network-sf0.003-bi-factors sample-data/
mv social-network-sf0.003-bi-composite-merged-fk sample-data/
