# Nightly Digest Release & Deployment Process

## Overview

The Nightly Digest application is composed of a front end ([ts_logging_frontend](https://github.com/lsst-ts/ts_logging_frontend)) and a back end ([ts_logging_and_reporting](https://github.com/lsst-ts/ts_logging_and_reporting)) that are tagged separately and deployed together in multiple Kubernetes environments for Rubin Observatory. New features are tested at usdf-rsp-dev.slac.stanford.edu/nightlydigest and our production environments are usdf-rsp.slac.stanford.edu/nightlydigest, Rubin's internal Base Test Stand and Summit, and a future public deployment.

## Prerequisites & Access

Code contributions and git tag creation are accepted from members of the lsst-ts github organization.
Edits to [ts_cycle_build](https://github.com/lsst-ts/ts_cycle_build) & associated Jenkins workflows require elevated permissions.
Access to Base Test Stand and our summit deployment requires summit software team membership.
Edit permissions to [Phalanx](https://github.com/lsst-sqre/phalanx) requires group membership.
Access to usdf-rsp locations requires at least a [SLAC](https://s3df.slac.stanford.edu/#/get-started) account.
Access to public deployment locations requires TBD permissions.

## Steps

### 1. Feature Development

1. Implement Ticket - Create Ticket branch, implement changes
2. Create Pull Request - Ticket author creates a Pull Request into `develop` branch
   1. Draft PRs indicate they are not ready to be tested or reviewed
3. Automated CI check - Tests and builds are sent to Jenkins & report pass/fail back to Pull Request

### 2. Create Dev Release

4. Determine what kind of release you'd like to create based on the type of changes you'd like to release.
   1. alpha release - use when work has not been merged to `develop` branch.
      1. Tag the head of the target branch
      2. (Or if releasing multiple branches) Create a branch (`alpha-release`) to merge, resolve conflicts, and tag release branch
   2. release candidate - use when changes are merged into `develop` branch but require further integration testing
      1. Tag `develop` using an `rc` marking: `vX.Y.Z-rc.N`
   3. production release - use when there has been a delay in production releases, or merged work has not been deployed to dev environment.
      1. Follow the production release process below.

- Note: See [Versioning from TSSW Developer Guide](https://tssw-developer.lsst.io/development-guidelines/versioning.html) for how to tag different releases.
- Note 2: Often the backend (ts_logging_and_reporting) receives a production tag and an alpha release, to remove some tests that are unable to run in our automated environment due to packaging availability (`lsst-resources` in particular is not conda packaged)

5. Create your chosen release and create it:
   1. Identify head of most recent changes or create/update release branch if alpha release - integrate all selected PRs onto the same branch `alpha-release`
   2. Create release notes - [run towncrier](https://tssw-developer.lsst.io/development-guidelines/language/python.html#version-history) notes & `python scripts/make_release.py` for front end (updates `package*.json`)
   3. Tag - Create tags on both front end and back end release branches

_We are actively automating parts of our dev release process and will update this section accordingly_
- When automated, should this section run per PR?
  - Would each new PR need to be rebased on top of the most recently dev-released unmerged PR, or would each overwrite previously tested PR to each test individually?

### 3. Deploy Dev Release

6. Await automated Conda build for the back end image to be created via Jenkins - automatically initiated with new git tags
8. Point `ts_cycle_build` current revision to new development tags - follow [`ts_cycle_build` release documentation](https://ts-cycle-build.lsst.io/user-guide/user-guide.html#building-a-new-revision) to update the package number in the current cycle and run builds
9. Deploy on Base Test Stand & test that no page or applet breaks
   1. Follow the Phalanx documentation to [sync Argo CD in that environment](https://phalanx.lsst.io/admin/sync-argo-cd.html)
   2. If any major breaks are found, look for and implement hotfixes
10. Deploy on usdf-rsp-dev & test new features
   1. Follow the Phalanx documentation to bounce Kubernetes pods
11. Announce in #osw-logging for people to test

### 4. Test Development Deployment

11. Test the deployed changes on usdf-rsp-dev per code review
12. Identify & communicate bugs to the PR author
    1. For merged PRs: Create Jira tickets linked to initial PRs/tickets
    2. Open PRs: Document the bug in the open Jira ticket & leave a comment in the PR referencing the ticket
13. Push bug fixes to the PR
    1. The bug fixes are only redeployed to the dev environment if another dev deployment is scheduled and the associated PR has not yet been merged
14. Approve & Merge PR into `develop` branch

### 5. Create Production Release

1. Check on step 12.1. first before continuing with the production release (bugs merged to not release)
2. Merge `develop` branch into `main` branch
3. On `main` create the release notes & git tag
   1. Create release notes - [run towncrier](https://tssw-developer.lsst.io/development-guidelines/language/python.html#version-history) notes & `python scripts/make_release.py` for front end
   2. Create production tags - on `main` branch, create a git tag (`v0.1.2`) to include most recent PRs
4. Create PR & merge `main` back into `develop`

### 6. Deploy to Production

3. Await automated Conda build for the back end image to be created via Jenkins - automatically initiated with new git tags
4. Add new tags to current `ts_cycle_build` revision - Follow [`ts_cycle_build` release documentation](https://ts-cycle-build.lsst.io/user-guide/user-guide.html#building-a-new-revision) to update the package number in the current cycle and run builds
   1. Update the Kubernetes values files to reference the new revision in the `image` `tag` -- follow [Phalanx documentation](https://phalanx.lsst.io/developers/helm-chart/values-yaml.html)
5. Deploy the new images to the production environments (usdf-rsp, summit, <public>) by bouncing the Kubernetes pods
   1. Follow the Phalanx documentation to [sync Argo CD in that environment](https://phalanx.lsst.io/admin/sync-argo-cd.html)
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
