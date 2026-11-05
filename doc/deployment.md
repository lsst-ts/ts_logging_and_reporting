# Deployment

This document describes how the Nightly Digest is deployed and how
each deployment is configured.

Deployments are defined in [Phalanx](https://github.com/lsst-sqre/phalanx)
([docs](https://phalanx.lsst.io/)), Rubin's repository of Kubernetes
configuration. Each application there is a Helm chart with a values
file per environment, and Argo CD applies them to each environment's
cluster. [release-process.md](release-process.md) covers how a new version gets
deployed.

---

## 1. Components

Each environment (summit, base, USDF and USDF dev) runs the Nightly
Digest as the Phalanx application `nightlydigest`, in the
`nightlydigest` namespace:

| Component | Image | Replicas | Role |
|---|---|---|---|
| nginx | `nginx` | 1 | Serves the frontend, and proxies and caches API requests. |
| API (`nightlydigest-backend`) | `nightlydigest-backend` | 4 | Serves the endpoints on port 8080, probed on `/health`. |
| Refresh worker (`nightlydigest-backend-worker`) | `nightlydigest-backend` | **exactly 1** | Keeps today's cache entries warm ([§12 of the service and adapter infrastructure doc](service-adapter-infrastructure.md#12-the-refresh-worker)). |
| Redis (`nightlydigest-redis`) | Redis chart from `lsst-sqre` | 1 | The cache shared by the API and the worker. |

The frontend has no server of its own: an init container copies its
built files from the `nightlydigest-frontend` image into nginx's
static store.

USDF-dev also runs an *alpha* API deployment,
built from the `alpha-release` branch
([release-process.md](release-process.md#b-semi-automatic-ci-pipeline)),
to test unmerged changes.

---

## 2. Configuration

Everything is configured in [Phalanx](https://github.com/lsst-sqre/phalanx),
under `applications/nightlydigest`:

| What | Where |
|---|---|
| Image tags, replicas, environment variables, mounts | `values-<environment>.yaml`, under `nightlydigest-backend` |
| nginx config, including the proxy and cache | `values-<environment>.yaml`, under `nightlydigest-nginx.nginxConfig` |
| Redis memory and eviction | `values.yaml`, under `redis` |
| The deployments themselves | `charts/nightlydigest-backend` and `charts/nightlydigest-nginx` |

Environment variables are set in three ways
([environment-variables.md](environment-variables.md) describes each
variable):

- **Plain values**, listed under `nightlydigest-backend.env` in each
  environment's values file.
- **Secrets**, listed under `nightlydigest-backend.envSecrets`, each
  naming a Kubernetes secret and key. These come from the
  `nightlydigest` secret, synced from Vault, and from the Gafaelfawr
  service token the chart issues for `bot-nightlydigest-backend`. 
  The Vault secret is also mounted as files under `/etc/secrets`.
- **Fixed in the chart templates**, for values that are the same in
  every environment, such as the Redis service name.

Anything not set runs on its default.

Production environments run the release image tagged with the cycle
revision ([release-process.md](release-process.md#deploy-to-production));

USDF-dev runs the `develop` image.
