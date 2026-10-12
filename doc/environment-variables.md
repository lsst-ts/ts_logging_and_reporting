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
| `ND_CACHING_DISABLE_REDIS` | unset | `redis_client.py` | Any value other than empty or `0` disables redis caching entirely and makes the refresh worker exit at startup. |
| `ND_CACHING_DISABLE_WORKER` | unset | `run_refresh_worker.py` | Any value other than empty or `0` makes the refresh worker exit at startup, leaving redis caching on. |
| `LOG_LEVEL` | `INFO` | `utils/logging_config.py` | Log level for the entire app. |

The API service needs all of these to be set correctly; the refresh worker
needs the same set, since it drives the same adapters.

Variables read by the frontend, nginx, or the local Docker Compose
stack are documented in the frontend repository's
[`docker/README.rst`](https://github.com/lsst-ts/ts_logging_frontend/blob/develop/docker/README.rst).

---

## 2. Obtaining credentials

Each token is sent upstream exactly as set, so it must already be in
the form the upstream expects.

- **`ACCESS_TOKEN`**: on the RSP deployment named by
  `EXTERNAL_INSTANCE_URL`, open the account menu → *Security tokens* →
  *Create Token*, with at least the `image:read` and
  `exec:internal-tools` scopes. A token works only on the deployment
  that issued it.
- **`JIRA_API_TOKEN`**: create an API token at
  <https://id.atlassian.com/manage-profile/security/api-tokens>, then
  set the variable to the Base64 encoding of `<your Atlassian email>:<token>`:
  ```shell
  printf '%s' 'you@example.org:<token>' | base64 -w0
  ```
  `JIRA_API_HOSTNAME` is `rubinobs.atlassian.net`.
- **`ZEPHYR_API_TOKEN`**: in Jira, open the account menu → *Zephyr
  Scale API Access Tokens* → *Create access token*.
- **`AWS_SHARED_CREDENTIALS_FILE`**: needed only when working on
  expected exposures; without it, run with `ND_DEBUG` set and expected
  exposures reports zero. See [AWS credentials](#aws-credentials) below.

In deployments none of these are set by hand: Phalanx issues
`ACCESS_TOKEN` as a Gafaelfawr service token; other credentials
are set from Vault.

### AWS credentials

> **These credentials are sensitive.** Leaking them is a high-risk
> security incident.

1. In an RSP notebook, print your AWS credentials file:
   ```shell
   cat ~/.lsst/aws-credentials.ini
   ```
2. Copy **ONLY** the `[default]` profile. **DO NOT** copy any other
   profile; the `[embargo]` profile in particular is critical and must
   be left alone.
3. Save it in a safe place on your machine, readable only by you:
   ```shell
   chmod 600 path/to/credentials
   ```
4. In the .env, set `AWS_SHARED_CREDENTIALS_FILE` to that path, and
   `S3_ENDPOINT_URL` to `https://s3dfrgw.slac.stanford.edu`.

**Never commit this file.** If it is ever pushed to GitHub, report it
immediately: Git history is preserved, and public, even after the
commit is deleted.

---

## 3. Startup check

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
