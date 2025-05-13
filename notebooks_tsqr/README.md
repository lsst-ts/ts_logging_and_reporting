# Logging & Reporting Times Square Notebooks

Notebooks found here `./notebooks/` are meant to be run in Times Square towards use in project-wide Nightly Logging & Reporting
Times-Square: <https://usdf-rsp-dev.slac.stanford.edu/times-square>

See [official Times-Square documentation](https://rsp.lsst.io/v/usdfdev/guides/times-square/index.html) on creating notebooks for use by Times Square.

## Development Guidelines

Rapid Prototyping is enabled with the branch `prototype`  
Times-Square for this repository displays the `prototype` branch.

- Create a branch for your Jira Ticket in the format `tickets/dm-####` off of `prototype`
- Communicate often with team mate when you want to push changes to `prototype`
- Rebase your branch off `prototype` before merging your branch into `prototype`

Example of flow:

1. `git checkout prototype; git pull`
2. `git checkout -b tickets/dm-23456`
3. `git commit -m "work happened"; git push`
4. `git checkout prototype; git pull`
5. `git checkout tickets/dm-23456`
6. `git rebase prototype`
7. `git checkout prototype; git merge tickets/dm-23456; git push`

&nbsp;

Once Per Sprint (2 week), the developers on this repository (Steve Pothier & Valerie Becker) gather to discuss updates made to `prototype`, outstanding pull requests, and tickets that have been completed.  

Once they are in agreement, they merge `prototype` into the `develop` branch and close the related Jira Tickets. Squash commit should be used here with a descriptive title and description in the PR.


## NightLog.ipynb

NightLog.ipynb is our main Logging And Reporting notebook. This notebook is meant to display completed* views of logging information.
Each separate notebook should be used to mature a logging/reporting product, and then expect to be integrated into this 'main' notebook.

\*_Completed to an alpha\beta level -- quick improvements will continue to happen during Fall-Winter 2024_

## Dashboard

Dashboard.ipynb is intended for local development purposes and debugging. Run this notebook not from RSP to evaluate your connection to an array of data sources.  
_RSP is not intended to have access to all of the data sources queried here._

## Kernel

Times Square developers/maintainers have indicated that the LSST Kernel should be used in notebooks displayed there.  
[RSP Stack info](https://developer.lsst.io/stack/conda.html#rubin-science-platform-notebooks)

## Backend Code

We are working our way into a non-Times-Square dependent project. Towards that effort, we are incrementally abstracting common code out of the notebooks. This code is kept in `./python/lsst/ts/logging_and_reporting/`

`almanac.py` ...
`reports.py` ...
`source_adapters.py` ....
`utils.py` is used for ...