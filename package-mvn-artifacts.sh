#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

git clean -xdf .
mvn clean deploy
cp -r target/snb-mvn/* ../snb-mvn/
