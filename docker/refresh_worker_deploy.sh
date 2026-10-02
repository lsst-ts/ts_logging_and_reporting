#!/bin/bash
source $HOME/.setup_sal_env.sh

set -eu

# Run the ts_logging_and_reporting worker
run_refresh_worker &

pid="$!"

wait ${pid}
