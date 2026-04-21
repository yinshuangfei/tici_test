#/bin/bash

set -euo pipefail

CURR_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$CURR_DIR" || exit

echo "change directory to: $CURR_DIR"

cd ../../
tar czvf tici_test-`date +%F`.tar.gz tici_test
