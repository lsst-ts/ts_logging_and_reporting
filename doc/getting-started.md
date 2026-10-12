# Getting started

This document describes how to run the backend locally, including running its 
tests and lint, as well as configuring pre-commit

---

## 1. Prerequisites

- Docker with Compose.
- Conda, e.g. [Miniforge](https://github.com/conda-forge/miniforge).
- This repository and
  [ts_logging_frontend](https://github.com/lsst-ts/ts_logging_frontend)
  cloned side by side. The local development stack is defined in the
  frontend repository, and its example configuration assumes the two
  are siblings.
- An account on the RSP deployment you want to read data from, most likely
  [usdf-dev](https://usdf-rsp-dev.slac.stanford.edu/).

---

## 2. The local stack

`docker/docker-compose.yaml` in the frontend repository runs the whole
application:

| Service | Purpose | Port |
|---|---|---|
| `backend` | The API, built from this repository's `docker/Dockerfile` | 8080 |
| `refresh-worker` | Keeps today's cache entries warm (see §12 of [service-adapter-infrastructure.md](service-adapter-infrastructure.md#12-the-refresh-worker)) | — |
| `redis` | The cache | 6379 |
| `frontend` | The Vite dev server | 5173 |
| `nginx` | Serves the frontend and proxies `/nightlydigest/api/` to the backend | 80 |


### Configure

In the frontend repository, copy `docker/example_env` to `docker/.env`
and fill it in:

| Variable | Where it comes from |
|---|---|
| `EXTERNAL_INSTANCE_URL` | One of the known deployment URLs ([§15 of the service and adapter infrastructure doc](service-adapter-infrastructure.md#15-upstream-authentication-and-server-resolution)), most likely `https://usdf-rsp-dev.slac.stanford.edu` |
| `ACCESS_TOKEN` | An RSP token created on the deployment named by `EXTERNAL_INSTANCE_URL`, with at least the `read:image` and `exec:internal-tools` scopes. |
| `JIRA_API_TOKEN`, `JIRA_API_HOSTNAME` | A Jira API token and the Jira host. |
| `ZEPHYR_API_TOKEN` | Jira account menu → "Zephyr Scale API Access Tokens". |
| `LOCAL_RUBIN_SIM_DATA_DIR` | A local copy of the `rubin_sim` data, fetched with `rs_download_data`. |

[environment-variables.md](environment-variables.md) describes how to
obtain each token, all variables the backend reads, and what the
startup check enforces.

Leave `ND_DEBUG=1` set, as the example does. Amongst other things, it
enables the FastAPI automatic reload, making development easier.
The frontend's `docker/README.rst` covers the frontend-side and
Compose-only options.

### Run

From the frontend repository, run:

```shell
docker compose -f docker/docker-compose.yaml up [--build]
```

| URL | What |
|---|---|
| `http://localhost/nightlydigest/` | The application |
| `http://localhost/nightlydigest/api/docs` | Interactive API reference, generated from the route docstrings |
| `http://localhost:8080/<endpoint>` | The backend directly, bypassing the nginx cache |

Both backend containers mount this repository as their working copy,
so the image only needs rebuilding when dependencies change. The API
reloads on code changes; however the refresh worker does not, so restart
it after changing an adapter:

```shell
docker compose -f docker/docker-compose.yaml restart refresh-worker
```

### Useful while developing

- `LOG_LEVEL=DEBUG` in `.env` logs every upstream request and cache
  lookup; see [logging.md](logging.md).
- `ND_CACHING_DISABLE_NGINX=1` and `ND_CACHING_DISABLE_REDIS=1` turn
  off the respective cache layer, to see uncached upstream behaviour.
- `ND_CACHING_DISABLE_WORKER=1`, stops the refresh worker; leave it
  enabled unless specifically working on the RefreshWorker
- Redis is published on the host, so `redis-cli` can inspect or delete
  cache entries ([§5 of the service and adapter infrastructure doc](service-adapter-infrastructure.md#5-cache-keys-and-the-redis-client)).
- To clear the nginx cache, use 
`sudo docker exec nightlydigest-nginx sh -c 'rm -rf /var/cache/nginx/nightlydigest/*'`
- To directly access the FastAPI container, use
`docker exec -it nightlydigest-backend /home/saluser/.setup_dev.sh`

---

## 3. Host environment

The local stack runs in Docker, but two things require a separate conda
environment on the host:
- downloading the `rubin_sim` data the containers mount
- configuring pre-commit

```shell
conda create -n nightlydigest -c lsstts -c conda-forge \
    rubin-sim=2.6 pre-commit ts-pre-commit-config
conda activate nightlydigest
```

Keep the `rubin-sim` version in step with the pin in `conda/meta.yaml`.

### Download the `rubin_sim` data

`rs_download_data` downloads into `RUBIN_SIM_DATA_DIR`:

```shell
export RUBIN_SIM_DATA_DIR=/path/to/rubin_sim_data
rs_download_data
```

Point `LOCAL_RUBIN_SIM_DATA_DIR` in the frontend's `docker/.env` at the
same path. After a `rubin-sim` upgrade, run `rs_download_data --update`
to fetch any datasets whose version has changed.

### Set up pre-commit

The hooks are generated from `.ts_pre_commit_config.yaml`. From the
root of this repository:

```shell
generate_pre_commit_conf
```

This writes `.pre-commit-config.yaml` (ignored by git) and installs the
git hook, so the checks run on every commit. Re-run it after
`.ts_pre_commit_config.yaml` changes, or after updating
`ts-pre-commit-config` (`conda update -n nightlydigest ts-pre-commit-config`)
to pick up newer hook versions.

---

## 4. Tests and lint

Tests run in the `lsstts/develop-env` image the backend container is
built from, to ensure consistency with CI.

```shell
# Open shell in running container
docker exec -it nightlydigest-backend /home/saluser/.setup_dev.sh

# Run pytest
pytest
```

Linting is run through the pre-commit hooks set up in
[§3](#set-up-pre-commit): ruff, towncrier, license headers, and YAML
and XML checks. These checks run on the changed files in each commit;
to check the whole repository, from the host environment run:

```shell
pre-commit run --all-files
```

---

## 5. Before opening a pull request

- Branch from `develop` as `tickets/<JIRA TICKET>` and open the pull
  request against `develop`.
- Add a news fragment for the ticket; see `doc/news/README.rst`.
- If the change adds an endpoint or adapter, follow the checklist in
  [§11 of the service and adapter infrastructure doc](service-adapter-infrastructure.md#11-adding-a-new-endpoint-what-you-must-override).
- If the change adds or alters an environment variable, update
  [environment-variables.md](environment-variables.md) and the frontend
  repository's `docker/example_env` and `docker/docker-compose.yaml`.

[release-process.md](release-process.md) covers what happens after
merge.
