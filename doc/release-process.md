# Nightly Digest Release & Deployment Process

## Overview

The Nightly Digest application is composed of a front end ([ts_logging_frontend](https://github.com/lsst-ts/ts_logging_frontend)) and a back end ([ts_logging_and_reporting](https://github.com/lsst-ts/ts_logging_and_reporting)) that are tagged separately and deployed together in multiple Kubernetes environments for Rubin Observatory. New features are tested at usdf-rsp-dev.slac.stanford.edu/nightlydigest and our production environments are usdf-rsp.slac.stanford.edu/nightlydigest, (sometimes Rubin's summit cluster), and a future public deployment.

- potentially we should not put the full url to our internal deployments here

## Prerequisites & Access

Code contributions and git tag creation are accepted from members of the lsst-ts github organization.
Edits to [ts_cycle_build](https://github.com/lsst-ts/ts_cycle_build) & associated Jenkins workflows require elevated permissions.
Access to Base Test Stand and our summit deployment requires summit software team membership.
Edit permissions to [Phalanx](https://github.com/lsst-sqre/phalanx) requires group membership.
Access to usdf-rsp locations requires at least a [SLAC](https://s3df.slac.stanford.edu/#/get-started) account.
Access to public deployment locations requires EPO permissions.

## Steps

### 1. Feature Development

1. Implement Ticket - Create Ticket branch, implement changes
2. Create Pull Request - Ticket author creates a Pull Request into `develop` branch
3. Automated CI check - Tests and builds are sent to Jenkins & report pass/fail back to Pull Request

- < how should we indicate to each other that our PR is ready for deployment to dev environment? >
   1. Currently we indicate during weekly meeting
   2. Comment on PR 'Ready for Dev deployment'?
   3. Comment on the ticket?
   4. Something else?

### 2. Create Dev Release

- When automated, should this section run per PR?
  - Would each new PR need to be rebased on top of the most recently dev-released unmerged PR, or would each overwrite previously tested PR to each test individually?

2. Tag `develop` branch so unreleased work may build on top of that (if PRs have been merged into `develop` branch and it has not been tagged)
3. Create or update release branch - integrate all selected PRs from step 4 onto the same branch
   1. If only one PR, the release branch can be the ticket branch.
   - Merging work on a release branch can cause difficult merge or rebase issues sometimes.
4. Create release notes - run towncrier notes & `make_release.py` for front end (updates `package*.json`)
5. Alpha tag - Create alpha tags (`v1.2.3-alpha.1`) on both front end and back end release branches


### 3. Deploy Dev Release

8. Point `ts_cycle_build` current revision to new development tags - follow `ts_cycle_build` release documentation to update and run builds
9. Deploy on Base Test Stand & test new features
   1. Follow the Phalanx documentation to bounce Kubernetes pods
   - Do we still need to test here?
10. Deploy on usdf-rsp-dev & test new features
   1. Follow the Phalanx documentation to bounce Kubernetes pods
11. Announce in #osw-logging for people to test

### 4. Test Development Deployment

11. Test the deployed changes on usdf-rsp-dev per code review
12. Identify & communicate bugs to the PR author (PR comments or #osw-logging channel)
    - How should we communicate not to release to production if a bug is merged into `develop` branch?
13. Push bug fixes to the PR
    - We don't redeploy on dev environment usually after bug fix
14. Approve & Merge PR into `develop` branch

### 5. Create Production Release

1. Check on step 12.1. first before continuing with the production release (bugs merged to not release)
2. Merge `develop` branch into `main` branch
3. On `main` create the release notes & git tag
    1. Create release notes and update internal metadata - Run towncrier in the back end & `make_release.py` in the front end
    2. Create production tags - on `develop` branch, create a git tag (`v0.1.2`) to include most recent PRs
4. Create PR & merge `main` back into `develop`

### 6. Deploy to Production

3. Await automated Conda build for the back end image to be created via Jenkins - automatically initiated with new git tags
4. Add new tags to current `ts_cycle_build` revision - Follow the `ts_cycle_build` documentation
   2. Update the Kubernetes values files to reference the new revision -- follow Phalanx documentation
5. Deploy the new images to the production environments (usdf-rsp, summit, <public>) by bouncing the Kubernetes pods
6. Test out the deployments to spot check for errors
7. Announce in #osw-logging for users

### Hot Fix process

- We don't have a process for this

## Rollback

- What is a big enough issue to require a roll back?
- We don't currently have a process for this

## References

The Rubin Summit Software team deploys software to the summit through a [cycle build process](https://ts-cycle-build.lsst.io/). The Nightly Digest is not a core package, and is can be [updated in revisions](https://ts-cycle-build.lsst.io/user-guide/user-guide.html#building-a-new-revision).

We use [Phalanx](https://phalanx.lsst.io/) to define our Kubernetes configurations and [deploy in internal environments](https://phalanx.lsst.io/developers/helm-chart/index.html).
