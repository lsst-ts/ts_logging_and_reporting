#!/usr/bin/env bash

source $HOME/.setup_sal_env.sh

set -eu

# Run the ts_logging_and_reporting refresh worker.
run_refresh_worker &

pid="$!"

wait ${pid}
