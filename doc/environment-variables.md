# Environment variables

This document lists the environment variables the backend reads, and
describes the check that enforces them at startup.

---

## 1. Variables

| Variable | Default | Read by | Purpose |
|---|---|---|---|
| `EXTERNAL_INSTANCE_URL` | *(none—required)* | `utils/auth.py` | Identifies the deployment and supplies the default upstream base URL. Must exactly match a known deployment URL ([§15 of the service and adapter infrastructure doc](service-adapter-infrastructure.md#15-upstream-authentication-and-server-resolution)). |
| `ACCESS_TOKEN` | *(none—required)* | `utils/auth.py` | RSP service-account token for deployment-local APIs and the `rubin_nights` clients. |
| `JIRA_API_TOKEN` | *(none—required)* | `utils/auth.py` | Jira credential, sent as Basic auth. |
| `JIRA_API_HOSTNAME` | *(none—required)* | `utils/auth.py` | Jira host; also used to build the BLOCK links in `/block-details` responses. |
| `ZEPHYR_API_TOKEN` | *(none—required)* | `utils/auth.py` | Zephyr Scale credential. |
| `RUBIN_SIM_DATA_DIR` | *(none—required in production)* | `rubin_sim`, `rubin_scheduler` | Directory holding the `rubin_sim` data used by the almanac and visit maps. |
| `AWS_SHARED_CREDENTIALS_FILE` | *(none—required in production)* | `rubin_sim.sim_archive` | Credentials for the S3 bucket holding the prenight simulations behind expected exposures. |
| `S3_ENDPOINT_URL` | *(none—required in production)* | `rubin_sim.sim_archive` | S3 endpoint for that bucket. |
| `LSST_DISABLE_BUCKET_VALIDATION` | *(none—required in production)* | `rubin_sim.sim_archive` | Set to `1` in every deployment. |
| `ND_DEBUG` | unset | `utils/env_check.py` | Any value other than empty or `0` switches on development mode. |
| `REDIS_HOST` | `localhost` | `redis_client.py` | Redis hostname (`redis` in the dev compose stack). Required in production. |
| `REDIS_PORT` | `6379` | `redis_client.py` | Redis port. |
| `REDIS_DB` | `0` | `redis_client.py` | Redis logical database number. |
| `ND_CACHING_DISABLE_NGINX` | unset | `frontend: docker/nginx.conf.template` | Any value other than empty or 0 disables the nginx cache |
| `ND_CACHING_DISABLE_REDIS` | unset | `redis_client.py` | Any value other than empty or `0` disables redis caching entirely and makes the refresh worker exit at startup. |
| `ND_CACHING_DISABLE_WORKER` | unset | `run_refresh_worker.py` | Any value other than empty or `0` makes the refresh worker exit at startup, leaving redis caching on. |
| `LOG_LEVEL` | `INFO` | `utils/logging_config.py` | Log level for the entire app. |

The API service needs all of these to be set correctly; the refresh worker
needs the same set, since it drives the same adapters.

---

## 2. Startup check

Both entrypoints (`run_logging_and_reporting` and `run_refresh_worker`)
call `check_environment()` (`utils/env_check.py`) before starting. It
exits with a single critical log line naming the problems it found if:

- a required variable is unset or blank;
- `EXTERNAL_INSTANCE_URL` is not a known deployment URL;
- a production-only variable is unset or blank, unless `ND_DEBUG` is set.

With `ND_DEBUG` set, unset production-only variables are logged as a
warning instead, and the features that depend on them fail at runtime.
Deployments leave `ND_DEBUG` unset, so production is the default; it is
meant for local development, where some credentials or data files may be
missing.
