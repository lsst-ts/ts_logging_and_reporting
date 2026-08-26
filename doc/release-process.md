# Nightly Digest Release & Deployment Process

## Overview

The Nightly Digest application is composed of a front end ([ts_logging_frontend](https://github.com/lsst-ts/ts_logging_frontend)) and a back end ([ts_logging_and_reporting](https://github.com/lsst-ts/ts_logging_and_reporting)) that are tagged separately and deployed together in multiple Kubernetes environments for Rubin Observatory. New features are tested at usdf-rsp-dev.slac.stanford.edu/nightlydigest and our production environments are usdf-rsp.slac.stanford.edu/nightlydigest, Rubin's internal Base Test Stand and Summit, and a future public deployment.

## Prerequisites & Access

Code contributions and git tag creation are accepted from members of the [lsst-ts github organization](https://github.com/lsst-ts).
Edits to [ts_cycle_build](https://github.com/lsst-ts/ts_cycle_build) & associated Jenkins workflows require elevated permissions.
Access to Base Test Stand and our summit deployment requires summit software team membership.
Edit permissions to [Phalanx](https://github.com/lsst-sqre/phalanx) requires group membership.
Access to usdf-rsp locations requires at least a [SLAC](https://s3df.slac.stanford.edu/#/get-started) account.
Access to public deployment locations requires TBD permissions.

## Feature Development

Follow the [TSSW development workflow](https://tssw-developer.lsst.io/work_management/development_workflow.html).

1. Implement Ticket - Create Ticket branch, implement changes
2. Create Pull Request - Ticket author creates a Pull Request into `develop` branch
   1. Draft PRs indicate they are not ready to be tested or reviewed
3. Automated CI check - Tests and builds are sent to Jenkins & report pass/fail back to Pull Request

## Create Dev Release

4. Determine what kind of release you'd like to create based on the type of changes you'd like to release.
   1. `alpha` release — use when work has not been merged to `develop`.
      1. Tag the head of the target branch.
      2. If releasing changes from multiple branches, create a release branch (`alpha-release`) to merge, resolve conflicts, and tag from that branch.
   2. `release candidate (rc)` — use when changes are merged into `develop` but require further integration testing.
      1. Tag `develop` with an `rc` marker, e.g. `vX.Y.Z-rc.N`.
   3. `production` release — use when changes are ready to promote to `main` and deploy to production.
      1. Follow the production release process below.

See [Versioning from TSSW Developer Guide](https://tssw-developer.lsst.io/development-guidelines/versioning.html) for how to tag different releases.

Note: Often we create production and alpha releases for the backend ([ts_logging_and_reporting](https://github.com/lsst-ts/ts_logging_and_reporting)), to remove some tests that are unable to run in our automated environment due to packaging availability (`lsst-resources` in particular is not conda packaged)

5. Create your chosen release:
   1. Identify the HEAD of the most recent changes, or create/update a release branch for an `alpha` release.
      1. Integrate all selected PRs onto the release branch (for example, `alpha-release`).
   2. Create release notes
      1. Run [towncrier](https://tssw-developer.lsst.io/development-guidelines/language/python.html#version-history) and `python scripts/make_release.py` for the front end (updates `package*.json`).
   3. Make Git tags
      1. Create tags on both front end and back end release branches.

## Deploy Dev Release

6. Await automated Conda build for the backend image to be created via Jenkins - automatically initiated with new git tags
7. Point `ts_cycle_build` current revision to new development tags - follow [`ts_cycle_build` release documentation](https://ts-cycle-build.lsst.io/user-guide/user-guide.html#building-a-new-revision) to update the package number in the current cycle and run builds (see [cycle build](#cycle-build) context below)
8. Deploy on Base Test Stand & test that no page or applet breaks
    1. Follow the Phalanx documentation to [sync Argo CD in that environment](https://phalanx.lsst.io/admin/sync-argo-cd.html)
    2. If any major breaks are found, look for and implement [hotfixes](#hotfix)
9. Deploy on usdf-rsp-dev & test new features
    1. Follow the Phalanx documentation to bounce Kubernetes pods
10. Announce the release in #osw-logging for people to test

## Test Development Deployment

11. Test the deployed changes on usdf-rsp-dev per code review
12. Identify & communicate bugs to the PR author
    1. For merged PRs: Create Jira tickets linked to initial PRs/tickets
    2. Open PRs: Document the bug in the open Jira ticket & leave a comment in the PR referencing the ticket
13. Push bug fixes to the PR
    1. The bug fixes are only redeployed to the dev environment if another dev deployment is scheduled and the associated PR has not yet been merged
14. Approve & Merge PR into `develop` branch

## Create Production Release

1. Check on step 12.1. first before continuing with the production release (don't release merged bugs merged)
2. Merge `develop` branch into `main` branch
3. On `main` create the release notes & git tag
   1. Create release notes - [run towncrier](https://tssw-developer.lsst.io/development-guidelines/language/python.html#version-history) notes & `python scripts/make_release.py` for front end
   2. Create production tags - on `main` branch, create a git tag (`v0.1.2`) to include most recent PRs
4. Create PR & merge `main` back into `develop`

## Deploy to Production

5. Await automated Conda build for the back end image to be created via Jenkins - automatically initiated with new git tags
6. Add new tags to current `ts_cycle_build` revision (see [References](#references))
   1. Follow [`ts_cycle_build` release documentation](https://ts-cycle-build.lsst.io/user-guide/user-guide.html#building-a-new-revision) to update the package number in the current cycle and run builds
7. Update the Phalanx application values files to reference the new revision in the `image` `tag` -- follow [Phalanx documentation](https://phalanx.lsst.io/developers/helm-chart/values-yaml.html)
8. Deploy the new images to the production environments (usdf-rsp, summit, <public>) by bouncing the Kubernetes pods
   1. Follow the Phalanx documentation to [sync Argo CD in that environment](https://phalanx.lsst.io/admin/sync-argo-cd.html)
9. Test out the deployments to spot check for errors
10. Announce in `#osw-logging` for users.

## HotFix

We don't typically create hotfix releases; however, if there is a quick fix found while testing we suggest creating an `alpha` tag and follow the [Create Dev Release process](#create-dev-release) above with the `alpha` version.
If you are confident the change will work, use a branch & Pull Request process as normal to merge your work into `develop`. (allow skipping of the dev environment deployment)

## Rollback

The Nightly Digest is a dashboard of summary metrics without many packages depending on it. For this reason, we have a large roll back margin.
**When something is broken, tend towards a hotfix.**

If many summary page applets are broken, or a single full additional page is broken, rollback to a previous version. Naturally, the production deployment should be more functional than our development environment where we are testing new features, so leaving dev deployment broken for up to a week is acceptable.

- If the fix is quick (< 1 day) create a hotfix and release the new changes into production.
- If the fix or process of debugging tends towards a longer term (> 1 day) [roll back production](#rollback-production-deployment).

However, if the issue creates abnormal load on any of the data sources we query or our infrastructure, no matter how quick the fix, rollback immediately.

### Rollback Dev Deployment

Follow the steps in [Deploy Dev Release](#deploy-dev-release) by rebuilding the target tag in Jenkins. Continue from there, redeploying the previous tag to the dev environment.

### Rollback Production Deployment

Follow the steps in [Deploy to Production](#deploy-to-production) starting with updating the Phalanx environment files back to the previous cycle revision tag. (i.e. c0043.006 -> c0043.002 or whichever the previous stable revision number was)

## References

### Phalanx

We use [Phalanx](https://phalanx.lsst.io/) to define our Kubernetes configurations and [deploy in internal environments](https://phalanx.lsst.io/developers/helm-chart/index.html).

### Cycle Build

The Rubin Summit Software team deploys software to the summit through a [cycle build process](https://ts-cycle-build.lsst.io/) to control and coordinate compatibility between the different components of the observatory control system. The Nightly Digest is not a core package, and can be [updated in revisions](https://ts-cycle-build.lsst.io/user-guide/user-guide.html#building-a-new-revision).

Whenever a core component of the control system is released we begin a new cycle to align the rest of the core packages.

- Nightly Digest is not a core package and therefore does not drive new cycles but can be updated via revisions.

- We are included in the cycle build process because we deploy to the summit


The `ts_cycle_build` repository holds the package versions and dockerfiles for packages included in this process.

When Nightly Digest releases a new version, we coordinate with the Summit Software team via Slack to understand the status of the current revision, update that revision branch in `ts_cycle_build`, and create our associated images through Jenkins.

This process builds two images, one with a cycle tag (c0045) and one including the revision tag (c0045.002). We use the less specific tag (cycle only, c0045) for our dev deployments, and the further specified (cycle & revision c0045.002) for production

When we create a new version of our package, we check the cycle build slack channel to understand the status of the cycle, and maybe create a new revision if necessary. If there is a cycle upgrade in progress already, we usually refrain from adding our non-core package version upgrade until testing the cycle has become stable.
