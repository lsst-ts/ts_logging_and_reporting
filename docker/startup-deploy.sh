#!/usr/bin/env bash

source $HOME/.setup_sal_env.sh

set -eu

# Run the ts_logging_and_reporting FastAPI web server.
run_logging_and_reporting &

pid="$!"

wait ${pid}
